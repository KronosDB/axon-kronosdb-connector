package io.kronosdb.connector.transport;

import io.kronosdb.protocol.Ack;
import io.kronosdb.protocol.Command;
import io.kronosdb.protocol.CommandHandled;
import io.kronosdb.protocol.CommandResponse;
import io.kronosdb.protocol.CommandResult;
import io.kronosdb.protocol.Credit;
import io.kronosdb.protocol.DispatchCommand;
import io.kronosdb.protocol.ErrorDetail;
import io.kronosdb.protocol.Frame;
import io.kronosdb.protocol.HandleCommand;
import io.kronosdb.protocol.OpenCommandHandlers;
import io.kronosdb.protocol.ServerAck;
import io.kronosdb.protocol.SubscribeCommand;
import io.kronosdb.protocol.UnsubscribeCommand;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

/**
 * Channel abstraction for the KronosDB command bus of the native protocol.
 * Manages the handler stream (an {@code OpenCommandHandlers} stream) for
 * command handler registration and provides dispatch capability.
 */
public class CommandChannel {

    private static final Logger logger = LoggerFactory.getLogger(CommandChannel.class);

    // Flow control constants
    private static final long INITIAL_PERMITS = 500;
    private static final double REFILL_THRESHOLD = 0.25;

    private final Transport transport;
    private final String context;
    private final @Nullable String busName;

    private volatile @Nullable ClientStream handlerStream;
    private final Map<String, CompletableFuture<Ack>> pendingAcks = new ConcurrentHashMap<>();
    private volatile Function<Command, CompletableFuture<CommandResult>> commandHandler;

    // Tracked state for reconnection: everything needed to re-register after the stream drops.
    private volatile String registeredClientId;
    private volatile String registeredComponentName;
    /** Command name to load factor. */
    private final Map<String, Integer> activeSubscriptions = new ConcurrentHashMap<>();
    private volatile boolean reconnecting = false;
    private final Backoff.Flaps flaps = new Backoff.Flaps();

    // Flow control: how many commands the server may still send.
    private final AtomicLong permitsRemaining = new AtomicLong(0);
    private volatile long currentPermitBatch = INITIAL_PERMITS;

    CommandChannel(Transport transport, String context, @Nullable String busName) {
        this.transport = transport;
        this.context = context;
        this.busName = busName;
    }

    /**
     * Dispatches a command to a registered handler and returns the result.
     */
    public CompletableFuture<CommandResult> sendCommand(Command command) {
        CompletableFuture<Frame> reply = transport.withBus(busName,
                (connection, bus) -> connection.request(new DispatchCommand(bus, command)));
        CompletableFuture<CommandResult> result = new CompletableFuture<>();
        reply.whenComplete((frame, error) -> {
            if (error != null) {
                result.completeExceptionally(Failures.unwrap(error));
            } else if (frame instanceof CommandResponse response) {
                result.complete(response.result());
            } else {
                result.completeExceptionally(new TransportException(
                        "the server answered with " + frame.getClass().getSimpleName() + " instead of CommandResponse"));
            }
        });
        return result;
    }

    /**
     * Registers a command handler for the given command name.
     * Opens a handler stream if not already open.
     *
     * @param handler      function that processes incoming commands
     * @param loadFactor   relative load factor for load balancing
     * @param commandNames the command type names this handler can process
     * @return a registration that can be cancelled to unsubscribe
     */
    public Registration registerCommandHandler(
            Function<Command, CompletableFuture<CommandResult>> handler,
            int loadFactor,
            String clientId,
            String componentName,
            String... commandNames) {
        this.commandHandler = handler;
        this.registeredClientId = clientId;
        this.registeredComponentName = componentName;

        ensureHandlerStreamOpen();

        // Collect ack futures for all subscriptions (wait in parallel, not sequentially)
        List<CompletableFuture<?>> ackFutures = new ArrayList<>();

        for (String commandName : commandNames) {
            activeSubscriptions.put(commandName, loadFactor);
            CompletableFuture<Ack> ackFuture = sendSubscription(commandName, loadFactor);
            if (ackFuture != null) {
                ackFutures.add(ackFuture);
            }
        }

        // All acks resolve in parallel — worst case is one 5s timeout, not N * 5s
        CompletableFuture<Void> registrationAck = CompletableFuture.allOf(
                ackFutures.toArray(new CompletableFuture[0]));

        return new Registration(() -> {
            for (String commandName : commandNames) {
                activeSubscriptions.remove(commandName);
            }
            for (String commandName : commandNames) {
                sendToStream(new UnsubscribeCommand(UUID.randomUUID().toString(), commandName));
            }
        }, registrationAck);
    }

    private void sendToStream(Frame frame) {
        ClientStream stream = handlerStream;
        if (stream != null) {
            try {
                stream.send(frame);
            } catch (RuntimeException e) {
                logger.debug("Could not send {} on the command handler stream: {}.",
                        frame.getClass().getSimpleName(), e.getMessage());
            }
        }
    }

    private @Nullable CompletableFuture<Ack> sendSubscription(String commandName, int loadFactor) {
        if (handlerStream != null) {
            String instructionId = UUID.randomUUID().toString();
            CompletableFuture<Ack> ackFuture = new CompletableFuture<>();
            pendingAcks.put(instructionId, ackFuture);

            sendToStream(new SubscribeCommand(instructionId, commandName, loadFactor));

            // Time out the ack after 5 seconds so we don't block forever; the timeout
            // is converted into a synthetic success ack so registration proceeds.
            return ackFuture.orTimeout(5, TimeUnit.SECONDS)
                    .exceptionally(ex -> {
                        pendingAcks.remove(instructionId);
                        logger.debug("Ack timeout for command subscription [{}] instruction [{}].",
                                commandName, instructionId);
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
            logger.debug("Refilled {} command permits for context [{}]. Remaining: {}.",
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
                        new OpenCommandHandlers(bus, clientId, componentName), false, new HandlerListener())));
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
            throw new TransportException("interrupted while opening the command handler stream", e);
        } catch (java.util.concurrent.ExecutionException | java.util.concurrent.TimeoutException e) {
            Throwable cause = Failures.unwrap(e);
            throw new TransportException("could not open the command handler stream: " + cause.getMessage(), cause);
        }
    }

    /** Receives what the server sends on the handler stream. Runs on the connection's reader thread. */
    private final class HandlerListener implements StreamListener {
        @Override
        public void onFrame(Frame frame, boolean last) {
            if (frame instanceof HandleCommand handle) {
                permitsRemaining.decrementAndGet();
                // Writes and handler code never run on the reader thread.
                transport.handlers().execute(() -> {
                    maybeRefillPermits();
                    handleIncomingCommand(handle.command());
                });
            } else if (frame instanceof ServerAck ack) {
                handleAck(ack.ack());
            }
            if (last) {
                onClosed("completed");
            }
        }

        @Override
        public void onFailure(Throwable cause) {
            logger.warn("Command handler stream error for context [{}]: {}.", context, cause.getMessage());
            onClosed("failed");
        }

        private void onClosed(String how) {
            logger.debug("Command handler stream {} for context [{}].", how, context);
            handlerStream = null;
            int shortLives = flaps.ended();
            if (shortLives >= 2) {
                logger.warn("The command handler stream for client [{}] ended {} times in a row soon after opening. "
                        + "Another instance or connection may be using the same client id.", registeredClientId, shortLives);
            }
            scheduleReconnect();
        }
    }

    private void scheduleReconnect() {
        if (activeSubscriptions.isEmpty()) {
            return;
        }
        // If the previous reconnect "succeeded" and then the stream died again, reconnecting
        // is false and we MUST start a new loop.
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
                        logger.info("Reconnecting command handler stream for context [{}] (attempt {}, delay {}ms).",
                                context, attempt, delay);
                        ensureHandlerStreamOpen();

                        if (handlerStream == null) {
                            logger.warn("Stream open returned but handlerStream is null — retrying.");
                            continue;
                        }

                        // Re-register all active subscriptions.
                        activeSubscriptions.forEach(this::sendSubscription);

                        // Verify the stream survived registration — if it failed during
                        // re-registration, handlerStream will be null again.
                        if (handlerStream == null) {
                            logger.warn("Stream died during re-registration for context [{}] — retrying.", context);
                            continue;
                        }

                        logger.info("Command handler stream reconnected for context [{}] with {} subscriptions.",
                                context, activeSubscriptions.size());
                        return;
                    } catch (Exception e) {
                        logger.warn("Command handler reconnect attempt {} failed for context [{}]: {}.",
                                attempt, context, e.getMessage());
                        handlerStream = null;
                    }
                }
            } finally {
                reconnecting = false;
            }
        });
    }

    private void handleIncomingCommand(Command command) {
        Function<Command, CompletableFuture<CommandResult>> handler = commandHandler;
        if (handler == null) {
            logger.warn("Received command [{}] but no handler is registered.", command.name());
            return;
        }

        CompletableFuture<CommandResult> result;
        try {
            result = handler.apply(command);
        } catch (RuntimeException e) {
            result = CompletableFuture.failedFuture(e);
        }
        result.whenComplete((response, error) -> {
            if (error != null) {
                Throwable cause = Failures.unwrap(error);
                logger.error("Error handling command [{}].", command.name(), cause);
                String message = cause.getMessage() != null ? cause.getMessage() : cause.toString();
                response = new CommandResult(
                        UUID.randomUUID().toString(),
                        command.messageId(),
                        "COMMAND_EXECUTION_ERROR",
                        new ErrorDetail(message, "", List.of(), "COMMAND_EXECUTION_ERROR"),
                        null,
                        List.of(),
                        List.of());
            }
            sendToStream(new CommandHandled(response));
        });
    }

    private void handleAck(Ack ack) {
        CompletableFuture<Ack> future = pendingAcks.remove(ack.instructionId());
        if (future != null) {
            if (!ack.success()) {
                logger.warn("KronosDB refused a command subscription: {}.",
                        ack.error() != null ? ack.error().message() : "no reason given");
            }
            future.complete(ack);
        }
    }

    /**
     * Prepares for disconnect by ending the handler stream, which unsubscribes everything on it.
     */
    public CompletableFuture<Void> prepareDisconnect() {
        activeSubscriptions.clear();
        ClientStream stream = handlerStream;
        handlerStream = null;
        if (stream != null) {
            stream.cancel();
        }
        return CompletableFuture.completedFuture(null);
    }
}
