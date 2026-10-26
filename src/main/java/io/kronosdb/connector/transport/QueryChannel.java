package io.kronosdb.connector.transport;

import io.kronosdb.protocol.Ack;
import io.kronosdb.protocol.CancelQuery;
import io.kronosdb.protocol.CancelSubscriptionQuery;
import io.kronosdb.protocol.Credit;
import io.kronosdb.protocol.DispatchQuery;
import io.kronosdb.protocol.Frame;
import io.kronosdb.protocol.HandleQuery;
import io.kronosdb.protocol.HandleSubscriptionQuery;
import io.kronosdb.protocol.OpenQueryHandlers;
import io.kronosdb.protocol.OpenSubscriptionQuery;
import io.kronosdb.protocol.Query;
import io.kronosdb.protocol.QueryDone;
import io.kronosdb.protocol.QueryHandled;
import io.kronosdb.protocol.QueryResponse;
import io.kronosdb.protocol.QueryResult;
import io.kronosdb.protocol.QueryUpdate;
import io.kronosdb.protocol.ServerAck;
import io.kronosdb.protocol.SubscribeQuery;
import io.kronosdb.protocol.SubscriptionEnd;
import io.kronosdb.protocol.SubscriptionEvent;
import io.kronosdb.protocol.SubscriptionFailure;
import io.kronosdb.protocol.SubscriptionQuery;
import io.kronosdb.protocol.SubscriptionQueryEvent;
import io.kronosdb.protocol.SubscriptionQueryHandled;
import io.kronosdb.protocol.SubscriptionQueryPermits;
import io.kronosdb.protocol.UnsubscribeQuery;
import io.kronosdb.protocol.ErrorDetail;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * Channel abstraction for the KronosDB query bus of the native protocol.
 * Manages the handler stream (an {@code OpenQueryHandlers} stream) for query
 * handler registration and provides dispatch capability.
 */
public class QueryChannel {

    private static final Logger logger = LoggerFactory.getLogger(QueryChannel.class);

    // Flow control constants
    private static final long INITIAL_PERMITS = 500;
    private static final double REFILL_THRESHOLD = 0.25;
    private static final long DEFAULT_SUBSCRIPTION_PERMITS = 64;

    private final Transport transport;
    private final String context;
    private final @Nullable String busName;

    private volatile @Nullable ClientStream handlerStream;
    private final Map<String, CompletableFuture<Ack>> pendingAcks = new ConcurrentHashMap<>();
    private volatile QueryHandler queryHandler;

    // Tracked state for reconnection.
    private volatile String registeredClientId;
    private volatile String registeredComponentName;
    /** Query name to result name. */
    private final ConcurrentHashMap<String, String> activeSubscriptions = new ConcurrentHashMap<>();
    private volatile boolean reconnecting = false;
    private final Backoff.Flaps flaps = new Backoff.Flaps();

    // Flow control: how many queries the server may still send.
    private final AtomicLong permitsRemaining = new AtomicLong(0);
    private volatile long currentPermitBatch = INITIAL_PERMITS;

    // Axon update handler registrations for server-forwarded subscription queries,
    // keyed by subscription identifier.
    private final Map<String, Registration> subscriptionQueryRegistrations = new ConcurrentHashMap<>();

    // The update senders of those subscription queries, which hold updates back until the
    // subscriber has granted credits for them.
    private final Map<String, UpdateSender> updateSenders = new ConcurrentHashMap<>();

    // Queries the server asked to stop, by request id, so a handler can notice.
    private final Map<String, AtomicBoolean> cancelledQueries = new ConcurrentHashMap<>();

    QueryChannel(Transport transport, String context, @Nullable String busName) {
        this.transport = transport;
        this.context = context;
        this.busName = busName;
    }

    /**
     * Dispatches a query and returns a stream of results.
     */
    public ResultStream<QueryResult> query(Query query) {
        ResultStream<QueryResult> results = new ResultStream<>(frame ->
                frame instanceof QueryResponse response ? response.result() : null);
        transport.<Void>withBus(busName, (connection, bus) -> {
            results.attach(connection.open(new DispatchQuery(bus, query), true, results.listener()));
            return CompletableFuture.completedFuture(null);
        }).whenComplete((ignored, error) -> {
            if (error != null) {
                results.fail(Failures.unwrap(error));
            }
        });
        return results;
    }

    /**
     * Opens a subscription query for initial result plus live updates.
     */
    public SubscriptionQueryResult subscriptionQuery(Query query, int bufferSize) {
        String subscriptionId = UUID.randomUUID().toString();
        long permits = bufferSize > 0 ? bufferSize : DEFAULT_SUBSCRIPTION_PERMITS;

        ResultStream<SubscriptionEvent> results = new ResultStream<>(frame ->
                frame instanceof SubscriptionQueryEvent event ? event.event() : null);
        SubscriptionQueryResult result = new SubscriptionQueryResult(results, subscriptionId, permits);
        transport.<Void>withBus(busName, (connection, bus) -> {
            results.attach(connection.open(
                    new OpenSubscriptionQuery(bus, new SubscriptionQuery(subscriptionId, query, permits)),
                    false,
                    results.listener()));
            return CompletableFuture.completedFuture(null);
        }).whenComplete((ignored, error) -> {
            if (error != null) {
                results.fail(Failures.unwrap(error));
            }
        });
        return result;
    }

    /**
     * Registers a query handler for the given query definition.
     */
    public Registration registerQueryHandler(QueryHandler handler, String queryName, String resultName,
                                             String clientId, String componentName) {
        this.queryHandler = handler;
        this.registeredClientId = clientId;
        this.registeredComponentName = componentName;

        ensureHandlerStreamOpen();

        activeSubscriptions.put(queryName, resultName);
        CompletableFuture<Ack> ackFuture = sendSubscription(queryName, resultName);

        // Build the registration ack future from the subscription ack
        CompletableFuture<Void> registrationAck;
        if (ackFuture != null) {
            registrationAck = ackFuture.thenApply(ack -> null);
        } else {
            registrationAck = CompletableFuture.completedFuture(null);
        }

        return new Registration(() -> {
            activeSubscriptions.remove(queryName);
            sendToStream(new UnsubscribeQuery(UUID.randomUUID().toString(), queryName, resultName));
        }, registrationAck);
    }

    private void sendToStream(Frame frame) {
        ClientStream stream = handlerStream;
        if (stream != null) {
            try {
                stream.send(frame);
            } catch (RuntimeException e) {
                logger.debug("Could not send {} on the query handler stream: {}.",
                        frame.getClass().getSimpleName(), e.getMessage());
            }
        }
    }

    private @Nullable CompletableFuture<Ack> sendSubscription(String queryName, String resultName) {
        if (handlerStream != null) {
            String instructionId = UUID.randomUUID().toString();
            CompletableFuture<Ack> ackFuture = new CompletableFuture<>();
            pendingAcks.put(instructionId, ackFuture);

            sendToStream(new SubscribeQuery(instructionId, queryName, resultName));

            // Time out the ack after 5 seconds; the timeout is converted into a
            // synthetic success ack so registration proceeds.
            return ackFuture.orTimeout(5, TimeUnit.SECONDS)
                    .exceptionally(ex -> {
                        pendingAcks.remove(instructionId);
                        logger.debug("Ack timeout for query subscription [{}] instruction [{}].",
                                queryName, instructionId);
                        return new Ack(instructionId, true, null);
                    });
        }
        return null;
    }

    private void grantPermits(long permits) {
        sendToStream(new Credit(permits));
    }

    /**
     * Checks if permits are running low and refills proactively.
     */
    private void maybeRefillPermits() {
        long remaining = permitsRemaining.get();
        long threshold = (long) (currentPermitBatch * REFILL_THRESHOLD);
        if (remaining <= threshold && registeredClientId != null) {
            long refill = currentPermitBatch;
            permitsRemaining.addAndGet(refill);
            grantPermits(refill);
            logger.debug("Refilled {} query permits for context [{}]. Remaining: {}.",
                    refill, context, permitsRemaining.get());
        }
    }

    private synchronized void ensureHandlerStreamOpen() {
        if (handlerStream != null) {
            return;
        }
        String clientId = registeredClientId;
        String componentName = registeredComponentName;
        CompletableFuture<ClientStream> opened = transport.withBus(busName, (connection, bus) ->
                CompletableFuture.completedFuture(connection.open(
                        new OpenQueryHandlers(bus, clientId, componentName), false, new HandlerListener())));
        try {
            handlerStream = opened.get(30, TimeUnit.SECONDS);
            flaps.opened();
            // Permits belong to the stream, whatever it subscribes to: one
            // batch per stream, granted as it opens.
            grantPermits(INITIAL_PERMITS);
            permitsRemaining.set(INITIAL_PERMITS);
            currentPermitBatch = INITIAL_PERMITS;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new TransportException("interrupted while opening the query handler stream", e);
        } catch (java.util.concurrent.ExecutionException | java.util.concurrent.TimeoutException e) {
            Throwable cause = Failures.unwrap(e);
            throw new TransportException("could not open the query handler stream: " + cause.getMessage(), cause);
        }
    }

    /** Receives what the server sends on the handler stream. Runs on the connection's reader thread. */
    private final class HandlerListener implements StreamListener {
        @Override
        public void onFrame(Frame frame, boolean last) {
            switch (frame) {
                case HandleQuery handle -> {
                    permitsRemaining.decrementAndGet();
                    // Writes and handler code never run on the reader thread.
                    transport.handlers().execute(() -> {
                        maybeRefillPermits();
                        handleIncomingQuery(handle.query(), null);
                    });
                }
                case ServerAck ack -> handleAck(ack.ack());
                case HandleSubscriptionQuery handle -> {
                    // Registered here, in the order the frames arrive, so credits that follow find it.
                    SubscriptionQuery subscription = handle.subscription();
                    UpdateSender sender = new UpdateSender(subscription.subscriptionId(), subscription.permits());
                    updateSenders.put(subscription.subscriptionId(), sender);
                    transport.handlers().execute(() -> handleSubscriptionQuery(subscription, sender));
                }
                case SubscriptionQueryPermits permits -> {
                    UpdateSender sender = updateSenders.get(permits.subscriptionId());
                    if (sender != null) {
                        transport.handlers().execute(() -> sender.grant(permits.permits()));
                    }
                }
                case CancelSubscriptionQuery cancel -> {
                    UpdateSender sender = updateSenders.remove(cancel.subscriptionId());
                    if (sender != null) {
                        sender.cancelled();
                    }
                    Registration registration = subscriptionQueryRegistrations.remove(cancel.subscriptionId());
                    if (registration != null) {
                        transport.handlers().execute(registration::cancel);
                    }
                }
                case CancelQuery cancel -> {
                    AtomicBoolean cancelled = cancelledQueries.get(cancel.requestId());
                    if (cancelled != null) {
                        cancelled.set(true);
                    }
                }
                default -> {
                    // QueryPermits bound what a handler may send ahead for a streaming query; this
                    // connector sends a query's results as it has them.
                }
            }
            if (last) {
                onClosed("completed");
            }
        }

        @Override
        public void onFailure(Throwable cause) {
            logger.warn("Query handler stream error for context [{}]: {}.", context, cause.getMessage());
            onClosed("failed");
        }

        private void onClosed(String how) {
            logger.debug("Query handler stream {} for context [{}].", how, context);
            handlerStream = null;
            int shortLives = flaps.ended();
            if (shortLives >= 2) {
                logger.warn("The query handler stream for client [{}] ended {} times in a row soon after opening. "
                        + "Another instance or connection may be using the same client id.", registeredClientId, shortLives);
            }
            cancelSubscriptionQueryRegistrations();
            scheduleReconnect();
        }
    }

    private void scheduleReconnect() {
        if (activeSubscriptions.isEmpty()) {
            return;
        }
        synchronized (this) {
            if (reconnecting) {
                return;
            }
            reconnecting = true;
        }

        transport.background(() -> {
            int attempt = flaps.startAttempt();
            try {
                while (!activeSubscriptions.isEmpty() && !transport.isClosed()) {
                    attempt++;
                    long delay = Backoff.delay(attempt);
                    try {
                        Thread.sleep(delay);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    }

                    try {
                        logger.info("Reconnecting query handler stream for context [{}] (attempt {}, delay {}ms).",
                                context, attempt, delay);
                        ensureHandlerStreamOpen();

                        if (handlerStream == null) {
                            logger.warn("Stream open returned but handlerStream is null — retrying.");
                            continue;
                        }

                        activeSubscriptions.forEach(this::sendSubscription);

                        if (handlerStream == null) {
                            logger.warn("Stream died during re-registration for context [{}] — retrying.", context);
                            continue;
                        }

                        logger.info("Query handler stream reconnected for context [{}] with {} subscriptions.",
                                context, activeSubscriptions.size());
                        return;
                    } catch (Exception e) {
                        logger.warn("Query handler reconnect attempt {} failed for context [{}]: {}.",
                                attempt, context, e.getMessage());
                        handlerStream = null;
                    }
                }
            } finally {
                reconnecting = false;
            }
        });
    }

    private void handleAck(Ack ack) {
        CompletableFuture<Ack> future = pendingAcks.remove(ack.instructionId());
        if (future != null) {
            future.complete(ack);
        }
    }

    /**
     * Runs a query on the handler and answers it on the handler stream: one {@code QueryHandled}
     * per result, then {@code QueryDone}. With {@code initialResult} set, the first result goes
     * there instead (the initial result of a subscription query) and nothing is sent on completion.
     */
    private void handleIncomingQuery(Query query, @Nullable Consumer<QueryResult> initialResult) {
        QueryHandler handler = queryHandler;
        if (handler == null) {
            logger.warn("Received query [{}] but no handler is registered.", query.name());
            return;
        }

        AtomicBoolean cancelled = new AtomicBoolean();
        if (initialResult == null) {
            cancelledQueries.put(query.messageId(), cancelled);
        }
        Consumer<QueryResult> sender = initialResult != null
                ? initialResult
                : result -> sendToStream(new QueryHandled(result));
        Runnable done = initialResult != null
                ? () -> { }
                : () -> {
                    cancelledQueries.remove(query.messageId());
                    sendToStream(new QueryDone(UUID.randomUUID().toString(), query.messageId()));
                };
        handler.handle(query, sender, done);
    }

    private void handleSubscriptionQuery(SubscriptionQuery subscription, UpdateSender updateSender) {
        QueryHandler handler = queryHandler;
        if (handler == null) {
            updateSenders.remove(subscription.subscriptionId(), updateSender);
            return;
        }
        String subscriptionId = subscription.subscriptionId();

        // 1. Register for updates — the handler will push updates back through the
        //    handler stream when projections emit them.
        Registration registration = handler.registerSubscriptionQuery(subscription, updateSender);
        subscriptionQueryRegistrations.put(subscriptionId, registration);

        // 2. Handle the initial query — the first result goes back as the initial_result,
        //    which the server forwards to the subscriber.
        AtomicBoolean sent = new AtomicBoolean();
        handleIncomingQuery(subscription.query(), result -> {
            if (sent.compareAndSet(false, true)) {
                sendToStream(new SubscriptionQueryHandled(subscriptionId, new SubscriptionEvent.OfInitialResult(result)));
            }
        });
    }

    private void cancelSubscriptionQueryRegistrations() {
        subscriptionQueryRegistrations.values().forEach(Registration::cancel);
        subscriptionQueryRegistrations.clear();
        updateSenders.values().forEach(UpdateSender::cancelled);
        updateSenders.clear();
    }

    /**
     * Sends subscription query updates back through the handler stream, within the credits the
     * subscriber has granted. The server drops an update the subscriber has no credit for, so
     * updates beyond them wait here, in order, until a {@code SubscriptionQueryPermits} frame
     * lets them go. A completion bypasses credits but still follows the updates before it.
     */
    private class UpdateSender implements SubscriptionUpdateSender {

        private final String subscriptionId;
        private final Object lock = new Object();
        private final ArrayDeque<SubscriptionEvent> held = new ArrayDeque<>();
        private long permits;
        private boolean finished;

        UpdateSender(String subscriptionId, long initialPermits) {
            this.subscriptionId = subscriptionId;
            // The wire carries an unsigned 64-bit count.
            this.permits = initialPermits < 0 ? Long.MAX_VALUE : initialPermits;
        }

        void grant(long more) {
            synchronized (lock) {
                permits = more < 0 || permits + more < 0 ? Long.MAX_VALUE : permits + more;
                flush();
            }
        }

        /** The subscriber left or the stream ended: nothing more is sent. */
        void cancelled() {
            synchronized (lock) {
                finished = true;
                held.clear();
            }
        }

        private void enqueue(SubscriptionEvent event) {
            synchronized (lock) {
                if (finished) {
                    return;
                }
                held.add(event);
                flush();
            }
        }

        private void flush() {
            while (!held.isEmpty()) {
                SubscriptionEvent next = held.peek();
                boolean update = next instanceof SubscriptionEvent.OfUpdate;
                if (update) {
                    if (permits <= 0) {
                        return;
                    }
                    permits--;
                }
                held.poll();
                sendToStream(new SubscriptionQueryHandled(subscriptionId, next));
                if (!update) {
                    finished = true;
                    held.clear();
                    updateSenders.remove(subscriptionId, this);
                }
            }
        }

        @Override
        public void sendUpdate(QueryUpdate update) {
            enqueue(new SubscriptionEvent.OfUpdate(update));
        }

        @Override
        public void complete(String clientId, String componentName) {
            enqueue(new SubscriptionEvent.OfComplete(new SubscriptionEnd(clientId, componentName)));
        }

        @Override
        public void completeExceptionally(String clientId, String componentName, String errorCode, String message) {
            enqueue(new SubscriptionEvent.OfCompleteExceptionally(new SubscriptionFailure(
                    clientId, componentName, errorCode,
                    new ErrorDetail(message, "", List.of(), errorCode))));
        }
    }

    /**
     * Prepares for disconnect by ending the handler stream, which unsubscribes everything on it.
     */
    public void prepareDisconnect() {
        activeSubscriptions.clear();
        ClientStream stream = handlerStream;
        handlerStream = null;
        if (stream != null) {
            stream.cancel();
        }
        cancelSubscriptionQueryRegistrations();
    }

    /**
     * Callback interface for handling incoming queries.
     */
    public interface QueryHandler {
        void handle(Query query,
                    Consumer<QueryResult> responseSender,
                    Runnable onComplete);

        /**
         * Registers a subscription query. The handler should call
         * {@code incomingHandler.registerUpdateHandler()} and bridge updates
         * back through the {@link SubscriptionUpdateSender}.
         */
        Registration registerSubscriptionQuery(SubscriptionQuery query, SubscriptionUpdateSender updateSender);
    }

    /**
     * Interface for sending subscription query updates back to KronosDB.
     */
    public interface SubscriptionUpdateSender {
        void sendUpdate(QueryUpdate update);

        void complete(String clientId, String componentName);

        void completeExceptionally(String clientId, String componentName, String errorCode, String message);
    }

    /**
     * Result of a subscription query, providing both a result stream and the ability
     * to send flow control / unsubscribe messages.
     */
    public static final class SubscriptionQueryResult {

        private final ResultStream<SubscriptionEvent> results;
        private final String subscriptionId;
        private final long initialPermits;
        private volatile boolean closed;

        SubscriptionQueryResult(ResultStream<SubscriptionEvent> results, String subscriptionId,
                                long initialPermits) {
            this.results = results;
            this.subscriptionId = subscriptionId;
            this.initialPermits = initialPermits;
        }

        public ResultStream<SubscriptionEvent> results() {
            return results;
        }

        public String subscriptionId() {
            return subscriptionId;
        }

        public long initialPermits() {
            return initialPermits;
        }

        /** Grants the handler this many more updates. */
        public void sendFlowControl(long permits) {
            if (closed) {
                return;
            }
            ClientStream stream = results.clientStream();
            if (stream == null) {
                return;
            }
            try {
                stream.send(new Credit(permits));
            } catch (RuntimeException e) {
                // Stream already finished (server closed or errored) — safe to ignore.
            }
        }

        public void close() {
            if (closed) {
                return;
            }
            closed = true;
            results.close();
        }
    }
}
