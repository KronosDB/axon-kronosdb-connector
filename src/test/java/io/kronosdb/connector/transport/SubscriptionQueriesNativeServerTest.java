package io.kronosdb.connector.transport;

import io.kronosdb.protocol.ErrorCode;
import io.kronosdb.protocol.MessageMetadata;
import io.kronosdb.protocol.MetadataValue;
import io.kronosdb.protocol.Payload;
import io.kronosdb.protocol.Query;
import io.kronosdb.protocol.QueryResult;
import io.kronosdb.protocol.QueryUpdate;
import io.kronosdb.protocol.SubscriptionEvent;
import io.kronosdb.protocol.SubscriptionQuery;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static io.kronosdb.connector.transport.RealServer.answer;
import static io.kronosdb.connector.transport.RealServer.await;
import static io.kronosdb.connector.transport.RealServer.eventually;
import static io.kronosdb.connector.transport.RealServer.query;
import static io.kronosdb.connector.transport.RealServer.text;
import static io.kronosdb.connector.transport.RealServer.unique;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Subscription queries through {@link QueryChannel} against a real KronosDB
 * server: the initial result, updates, completion, cancelling from either end,
 * and the subscriber's update credits. See {@link RealServer} for how to run
 * these.
 */
class SubscriptionQueriesNativeServerTest {

    private final List<Transport> transports = new CopyOnWriteArrayList<>();

    @BeforeEach
    void requireServer() {
        RealServer.require();
    }

    @AfterEach
    void close() {
        transports.forEach(Transport::close);
    }

    private Transport transport(String clientId) {
        Transport transport = RealServer.transport(clientId);
        transports.add(transport);
        return transport;
    }

    /** What a subscription query handler saw, and the senders it can push updates with. */
    private static final class Watching implements QueryChannel.QueryHandler {

        final Map<String, QueryChannel.SubscriptionUpdateSender> senders = new ConcurrentHashMap<>();
        final Map<String, SubscriptionQuery> subscriptions = new ConcurrentHashMap<>();
        final List<String> cancelled = new CopyOnWriteArrayList<>();

        @Override
        public void handle(Query query, Consumer<QueryResult> responseSender, Runnable onComplete) {
            responseSender.accept(answer(query.messageId(), "initial:" + text(query.payload())));
            onComplete.run();
        }

        @Override
        public Registration registerSubscriptionQuery(SubscriptionQuery query,
                                                      QueryChannel.SubscriptionUpdateSender updateSender) {
            subscriptions.put(query.subscriptionId(), query);
            senders.put(query.subscriptionId(), updateSender);
            return new Registration(() -> cancelled.add(query.subscriptionId()));
        }

        QueryChannel.SubscriptionUpdateSender only() throws Exception {
            eventually("a subscription reaches the handler", 10, () -> senders.size() == 1);
            return senders.values().iterator().next();
        }
    }

    private static QueryUpdate update(String text) {
        return new QueryUpdate(java.util.UUID.randomUUID().toString(),
                new Payload("Update", "1", java.nio.ByteBuffer.wrap(text.getBytes(java.nio.charset.StandardCharsets.UTF_8))),
                List.of(new MessageMetadata("seq", new MetadataValue.OfText(text))), "handler", "svc", "", null);
    }

    private Watching watcher(String name) throws Exception {
        Watching handler = new Watching();
        Transport transport = transport(unique("handler"));
        await(new QueryChannel(transport, "default", null)
                .registerQueryHandler(handler, name, "text", unique("client-watch"), "svc").ackFuture());
        return handler;
    }

    /** Opens a subscription, retrying until the handler's registration has reached the routing table. */
    private QueryChannel.SubscriptionQueryResult subscribe(QueryChannel subscriber, String name, int buffer)
            throws Exception {
        for (int attempt = 0; attempt < 200; attempt++) {
            QueryChannel.SubscriptionQueryResult result = subscriber.subscriptionQuery(query(name, "q"), buffer);
            SubscriptionEvent first = result.results().nextIfAvailable(2, TimeUnit.SECONDS);
            if (first != null) {
                // Hand the first event back to whoever reads next: tests read it as the initial result.
                pushedBack.put(result.subscriptionId(), first);
                return result;
            }
            if (result.results().getError().isEmpty()) {
                return result; // opened, handler slow to answer
            }
            result.close();
            Thread.sleep(50);
        }
        throw new AssertionError("the subscription never opened");
    }

    private final Map<String, SubscriptionEvent> pushedBack = new ConcurrentHashMap<>();

    private SubscriptionEvent next(QueryChannel.SubscriptionQueryResult result) throws Exception {
        SubscriptionEvent held = pushedBack.remove(result.subscriptionId());
        if (held != null) {
            return held;
        }
        SubscriptionEvent next = result.results().nextIfAvailable(10, TimeUnit.SECONDS);
        assertThat(next).as("an event within 10 seconds, error: %s", result.results().getError()).isNotNull();
        return next;
    }

    // --------------------------------------------------------- the stream

    @Test
    void aSubscriptionDeliversTheInitialResultThenUpdatesThenTheCompletion() throws Exception {
        String name = unique("Watch");
        Watching handler = watcher(name);
        QueryChannel subscriber = new QueryChannel(transport(unique("subscriber")), "default", null);

        QueryChannel.SubscriptionQueryResult result = subscribe(subscriber, name, 16);
        QueryChannel.SubscriptionUpdateSender sender = handler.only();

        assertThat(next(result)).isInstanceOfSatisfying(SubscriptionEvent.OfInitialResult.class, initial -> {
            assertThat(text(initial.value().payload())).isEqualTo("initial:q");
            assertThat(initial.value().requestId()).isEqualTo(result.subscriptionId());
        });
        sender.sendUpdate(update("one"));
        sender.sendUpdate(update("two"));
        sender.complete("handler", "svc");

        for (String expected : List.of("one", "two")) {
            assertThat(next(result)).isInstanceOfSatisfying(SubscriptionEvent.OfUpdate.class, update -> {
                assertThat(text(update.value().payload())).isEqualTo(expected);
                assertThat(update.value().metadata())
                        .containsExactly(new MessageMetadata("seq", new MetadataValue.OfText(expected)));
            });
        }
        assertThat(next(result)).isInstanceOf(SubscriptionEvent.OfComplete.class);
        eventually("the stream ends", 10, () -> result.results().isCompleted());
        assertThat(result.results().getError()).isEmpty();
    }

    @Test
    void theHandlerReceivesTheSubscriptionItsQueryAndTheSubscribersCreditWindow() throws Exception {
        String name = unique("Watch");
        Watching handler = watcher(name);
        QueryChannel subscriber = new QueryChannel(transport(unique("subscriber")), "default", null);

        QueryChannel.SubscriptionQueryResult result = subscribe(subscriber, name, 7);
        handler.only();

        SubscriptionQuery seen = handler.subscriptions.get(result.subscriptionId());
        assertThat(seen.permits()).isEqualTo(7);
        assertThat(seen.query().name()).isEqualTo(name);
        assertThat(text(seen.query().payload())).isEqualTo("q");
    }

    @Test
    void aHandlerThatFailsTheSubscriptionEndsItWithItsCodeAndMessage() throws Exception {
        String name = unique("Watch");
        Watching handler = watcher(name);
        QueryChannel subscriber = new QueryChannel(transport(unique("subscriber")), "default", null);
        QueryChannel.SubscriptionQueryResult result = subscribe(subscriber, name, 16);
        QueryChannel.SubscriptionUpdateSender sender = handler.only();
        assertThat(next(result)).isInstanceOf(SubscriptionEvent.OfInitialResult.class);

        sender.completeExceptionally("handler", "svc", "PROJECTION-9", "the projection broke");

        assertThat(next(result)).isInstanceOfSatisfying(SubscriptionEvent.OfCompleteExceptionally.class, failed -> {
            assertThat(failed.value().errorCode()).isEqualTo("PROJECTION-9");
            assertThat(failed.value().error().message()).isEqualTo("the projection broke");
        });
        eventually("the stream ends", 10, () -> result.results().isCompleted());
    }

    @Test
    void aSubscriptionNobodyServesFailsAsUnavailable() throws Exception {
        QueryChannel subscriber = new QueryChannel(transport(unique("subscriber")), "default", null);
        QueryChannel.SubscriptionQueryResult result = subscriber.subscriptionQuery(query(unique("Nobody"), "q"), 8);

        eventually("the subscription fails", 10, () -> result.results().isCompleted());
        assertThat(result.results().getError()).hasValueSatisfying(error -> assertThat(error)
                .isInstanceOfSatisfying(RequestFailedException.class,
                        e -> assertThat(e.code()).isEqualTo(ErrorCode.UNAVAILABLE)));
    }

    // ------------------------------------------------------------ cancel

    @Test
    void aSubscriberThatCancelsTellsItsHandler() throws Exception {
        String name = unique("Watch");
        Watching handler = watcher(name);
        QueryChannel subscriber = new QueryChannel(transport(unique("subscriber")), "default", null);
        QueryChannel.SubscriptionQueryResult result = subscribe(subscriber, name, 16);
        handler.only();

        result.close();

        eventually("the handler hears of the cancel", 10,
                () -> handler.cancelled.contains(result.subscriptionId()));
    }

    @Test
    void aHandlerThatCompletesCancelsNothingAndALaterSubscriptionStillWorks() throws Exception {
        String name = unique("Watch");
        Watching handler = watcher(name);
        QueryChannel subscriber = new QueryChannel(transport(unique("subscriber")), "default", null);
        QueryChannel.SubscriptionQueryResult first = subscribe(subscriber, name, 16);
        handler.only().complete("handler", "svc");
        assertThat(next(first)).isInstanceOf(SubscriptionEvent.OfInitialResult.class);
        assertThat(next(first)).isInstanceOf(SubscriptionEvent.OfComplete.class);

        QueryChannel.SubscriptionQueryResult second = subscribe(subscriber, name, 16);

        assertThat(next(second)).isInstanceOf(SubscriptionEvent.OfInitialResult.class);
        eventually("both subscriptions reached the handler", 10, () -> handler.senders.size() == 2);
    }

    @Test
    void aHandlerWhoseConnectionIsLostEndsItsSubscriptionsForTheSubscriber() throws Exception {
        String name = unique("Watch");
        Watching handler = new Watching();
        Transport handlerTransport = transport(unique("handler"));
        await(new QueryChannel(handlerTransport, "default", null)
                .registerQueryHandler(handler, name, "text", unique("client-watch"), "svc").ackFuture());
        QueryChannel subscriber = new QueryChannel(transport(unique("subscriber")), "default", null);
        QueryChannel.SubscriptionQueryResult result = subscribe(subscriber, name, 16);
        handler.only();
        assertThat(next(result)).isInstanceOf(SubscriptionEvent.OfInitialResult.class);

        handlerTransport.connection().close();

        // The server tells the subscriber the handler was lost (KRONOSDB-4006).
        assertThat(next(result)).isInstanceOfSatisfying(SubscriptionEvent.OfCompleteExceptionally.class,
                failed -> assertThat(failed.value().errorCode()).isEqualTo("KRONOSDB-4006"));
        eventually("the stream ends", 10, () -> result.results().isCompleted());
    }

    // ------------------------------------------------------- update credits

    @Test
    void updatesWithinTheSubscribersCreditsAllArriveAndMoreArriveWhenItGrantsMore() throws Exception {
        String name = unique("Watch");
        Watching handler = watcher(name);
        QueryChannel subscriber = new QueryChannel(transport(unique("subscriber")), "default", null);
        QueryChannel.SubscriptionQueryResult result = subscribe(subscriber, name, 4);
        QueryChannel.SubscriptionUpdateSender sender = handler.only();
        assertThat(next(result)).isInstanceOf(SubscriptionEvent.OfInitialResult.class);

        for (int i = 0; i < 4; i++) {
            sender.sendUpdate(update("a" + i));
        }
        List<String> got = new ArrayList<>(updates(result, 4));
        assertThat(got).containsExactly("a0", "a1", "a2", "a3");

        result.sendFlowControl(4);
        for (int i = 0; i < 4; i++) {
            sender.sendUpdate(update("b" + i));
        }
        got.addAll(updates(result, 4));
        assertThat(got).containsExactly("a0", "a1", "a2", "a3", "b0", "b1", "b2", "b3");
    }

    private List<String> updates(QueryChannel.SubscriptionQueryResult result, int count) throws Exception {
        List<String> texts = new ArrayList<>();
        while (texts.size() < count) {
            SubscriptionEvent event = next(result);
            assertThat(event).isInstanceOf(SubscriptionEvent.OfUpdate.class);
            texts.add(text(((SubscriptionEvent.OfUpdate) event).value().payload()));
        }
        return texts;
    }

    @Test
    void aHandlerThatEmitsFasterThanTheSubscriberGrantsLosesNoUpdates() throws Exception {
        String name = unique("Watch");
        Watching handler = watcher(name);
        QueryChannel subscriber = new QueryChannel(transport(unique("subscriber")), "default", null);
        QueryChannel.SubscriptionQueryResult result = subscribe(subscriber, name, 4);
        QueryChannel.SubscriptionUpdateSender sender = handler.only();
        assertThat(next(result)).isInstanceOf(SubscriptionEvent.OfInitialResult.class);

        // Five times the window, emitted at once. The subscriber tops its
        // credits up as it consumes, one at a time here.
        int total = 20;
        for (int i = 0; i < total; i++) {
            sender.sendUpdate(update("u" + i));
        }
        AtomicInteger consumed = new AtomicInteger();
        List<String> got = new ArrayList<>();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (got.size() < total && System.nanoTime() < deadline) {
            SubscriptionEvent event = result.results().nextIfAvailable(200, TimeUnit.MILLISECONDS);
            if (event instanceof SubscriptionEvent.OfUpdate update) {
                got.add(text(update.value().payload()));
                consumed.incrementAndGet();
                result.sendFlowControl(1);
            }
        }

        assertThat(got).as("every update, in order").hasSize(total)
                .containsExactlyElementsOf(java.util.stream.IntStream.range(0, total).mapToObj(i -> "u" + i).toList());
    }
}
