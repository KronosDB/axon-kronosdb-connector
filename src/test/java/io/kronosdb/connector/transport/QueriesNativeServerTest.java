package io.kronosdb.connector.transport;

import io.kronosdb.protocol.CancelQuery;
import io.kronosdb.protocol.Credit;
import io.kronosdb.protocol.ErrorCode;
import io.kronosdb.protocol.ErrorDetail;
import io.kronosdb.protocol.Frame;
import io.kronosdb.protocol.MessageMetadata;
import io.kronosdb.protocol.MetadataValue;
import io.kronosdb.protocol.OpenQueryHandlers;
import io.kronosdb.protocol.ProcessingInstruction;
import io.kronosdb.protocol.ProcessingKey;
import io.kronosdb.protocol.Query;
import io.kronosdb.protocol.QueryResult;
import io.kronosdb.protocol.SubscribeQuery;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static io.kronosdb.connector.transport.RealServer.answer;
import static io.kronosdb.connector.transport.RealServer.await;
import static io.kronosdb.connector.transport.RealServer.drain;
import static io.kronosdb.connector.transport.RealServer.eventually;
import static io.kronosdb.connector.transport.RealServer.query;
import static io.kronosdb.connector.transport.RealServer.text;
import static io.kronosdb.connector.transport.RealServer.unique;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Queries through {@link QueryChannel} against a real KronosDB server:
 * handlers, scatter-gather, cancelling, errors and recovery. See
 * {@link RealServer} for how to run these.
 */
class QueriesNativeServerTest {

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

    private QueryChannel channel(Transport transport) {
        return new QueryChannel(transport, "default", null);
    }

    /** A handler that answers each query with the given results, then is done. */
    private static QueryChannel.QueryHandler answering(java.util.function.Function<Query, List<QueryResult>> results) {
        return new QueryChannel.QueryHandler() {
            @Override
            public void handle(Query query, Consumer<QueryResult> responseSender, Runnable onComplete) {
                results.apply(query).forEach(responseSender);
                onComplete.run();
            }

            @Override
            public Registration registerSubscriptionQuery(io.kronosdb.protocol.SubscriptionQuery query,
                                                          QueryChannel.SubscriptionUpdateSender updateSender) {
                throw new UnsupportedOperationException();
            }
        };
    }

    /** A handler on its own transport that answers every {@code name} query with {@code tag}. */
    private Transport tagHandler(String name, String tag) throws Exception {
        Transport transport = transport(unique("handler-" + tag));
        Registration registration = channel(transport).registerQueryHandler(
                answering(query -> List.of(answer(query.messageId(), tag))),
                name, "text", unique("client-" + tag), "query-service");
        await(registration.ackFuture());
        return transport;
    }

    /** Asks until at least {@code expect} answers come back; registrations replicate asynchronously. */
    private static List<String> gather(QueryChannel dispatcher, String name, int expect) throws Exception {
        List<String> answers = List.of();
        for (int attempt = 0; attempt < 200; attempt++) {
            ResultStream<QueryResult> results = dispatcher.query(query(name, "?"));
            try {
                answers = drain(results).stream().map(r -> text(r.payload())).sorted().toList();
            } catch (Exception e) {
                // No handler yet.
            }
            if (answers.size() >= expect) {
                return answers;
            }
            Thread.sleep(50);
        }
        throw new AssertionError("the query never gathered " + expect + " answers, got " + answers);
    }

    private static Query withInstructions(Query query, ProcessingInstruction... instructions) {
        return new Query(query.messageId(), query.name(), query.timestamp(), query.payload(), query.metadata(),
                List.of(instructions), query.clientId(), query.componentName());
    }

    // ----------------------------------------------------- scatter-gather

    @Test
    void aQueryReachesItsHandlerWithItsMetadataAndTheAnswerComesBackBeforeTheEnd() throws Exception {
        String name = unique("FindOrders");
        List<Query> received = new CopyOnWriteArrayList<>();
        Transport handler = transport(unique("handler"));
        await(channel(handler).registerQueryHandler(answering(query -> {
            received.add(query);
            return List.of(new QueryResult("a-1", query.messageId(), "", null,
                    RealServer.payload("Orders", "wörld " + text(query.payload())),
                    List.of(new MessageMetadata("n", new MetadataValue.OfDouble(0.5)),
                            new MessageMetadata("hit", new MetadataValue.OfBoolean(true))),
                    List.of()));
        }), name, "orders", "client-find", "svc").ackFuture());
        QueryChannel dispatcher = channel(transport(unique("dispatcher")));
        gather(dispatcher, name, 1);

        Query sent = new Query("q-" + name, name, System.currentTimeMillis(), RealServer.payload(name, "älska"),
                List.of(new MessageMetadata("tenant", new MetadataValue.OfText("t1"))),
                List.of(new ProcessingInstruction(ProcessingKey.PRIORITY, new MetadataValue.OfNumber(2))),
                "caller-1", "web");
        ResultStream<QueryResult> results = dispatcher.query(sent);
        List<QueryResult> got = drain(results);

        assertThat(results.getError()).isEmpty();
        assertThat(got).hasSize(1);
        QueryResult result = got.get(0);
        assertThat(result.messageId()).isEqualTo("a-1");
        assertThat(result.requestId()).isEqualTo("q-" + name);
        assertThat(result.payload().typeName()).isEqualTo("Orders");
        assertThat(text(result.payload())).isEqualTo("wörld älska");
        assertThat(result.metadata()).containsExactlyInAnyOrder(
                new MessageMetadata("n", new MetadataValue.OfDouble(0.5)),
                new MessageMetadata("hit", new MetadataValue.OfBoolean(true)));

        Query arrived = received.stream().filter(q -> q.messageId().equals("q-" + name)).findFirst().orElseThrow();
        assertThat(arrived.name()).isEqualTo(name);
        assertThat(arrived.clientId()).isEqualTo("caller-1");
        assertThat(arrived.metadata()).containsExactly(new MessageMetadata("tenant", new MetadataValue.OfText("t1")));
        assertThat(arrived.instructions())
                .containsExactly(new ProcessingInstruction(ProcessingKey.PRIORITY, new MetadataValue.OfNumber(2)));
    }

    @Test
    void aScatterGatherQueryCollectsEveryHandlersAnswer() throws Exception {
        String name = unique("Count");
        tagHandler(name, "one");
        tagHandler(name, "two");
        tagHandler(name, "three");
        QueryChannel dispatcher = channel(transport(unique("dispatcher")));

        assertThat(gather(dispatcher, name, 3)).containsExactly("one", "three", "two");
    }

    @Test
    void askingForOneResultReachesOnlyOneHandler() throws Exception {
        String name = unique("CountOne");
        tagHandler(name, "one");
        tagHandler(name, "two");
        QueryChannel dispatcher = channel(transport(unique("dispatcher")));
        gather(dispatcher, name, 2);

        for (int i = 0; i < 5; i++) {
            Query single = withInstructions(query(name, "?"),
                    new ProcessingInstruction(ProcessingKey.NR_OF_RESULTS, new MetadataValue.OfNumber(1)));
            assertThat(drain(dispatcher.query(single))).hasSize(1);
        }
    }

    @Test
    void aHandlerThatAnswersSeveralTimesSendsEveryResultBeforeTheEnd() throws Exception {
        String name = unique("Pages");
        Transport handler = transport(unique("handler"));
        await(channel(handler).registerQueryHandler(
                answering(query -> List.of(answer(query.messageId(), "page-1"), answer(query.messageId(), "page-2"),
                        answer(query.messageId(), "page-3"))),
                name, "text", "client-pages", "svc").ackFuture());
        QueryChannel dispatcher = channel(transport(unique("dispatcher")));

        assertThat(gather(dispatcher, name, 3)).containsExactly("page-1", "page-2", "page-3");
    }

    @Test
    void aHandlersOwnErrorIsAResultNotAFailureOfTheQuery() throws Exception {
        String name = unique("Fragile");
        Transport handler = transport(unique("handler"));
        await(channel(handler).registerQueryHandler(
                answering(query -> List.of(new QueryResult("e-1", query.messageId(), "APP-7",
                        new ErrorDetail("no such order", "orders", List.of("id 9"), "APP-7"), null,
                        List.of(), List.of()))),
                name, "text", "client-fragile", "svc").ackFuture());
        QueryChannel dispatcher = channel(transport(unique("dispatcher")));

        List<QueryResult> results = List.of();
        for (int attempt = 0; attempt < 100 && results.isEmpty(); attempt++) {
            results = drain(dispatcher.query(query(name, "?")));
            Thread.sleep(50);
        }

        assertThat(results).hasSize(1);
        assertThat(results.get(0).errorCode()).isEqualTo("APP-7");
        assertThat(results.get(0).error()).isEqualTo(new ErrorDetail("no such order", "orders", List.of("id 9"), "APP-7"));
        assertThat(results.get(0).payload()).isNull();
    }

    @Test
    void aQueryNobodyHandlesFailsAsUnavailable() throws Exception {
        QueryChannel dispatcher = channel(transport(unique("dispatcher")));
        ResultStream<QueryResult> results = dispatcher.query(query(unique("Nobody"), "?"));

        eventually("the query fails", 10, results::isCompleted);
        assertThat(results.getError()).hasValueSatisfying(error -> assertThat(error)
                .isInstanceOfSatisfying(RequestFailedException.class,
                        e -> assertThat(e.code()).isEqualTo(ErrorCode.UNAVAILABLE)));
    }

    @Test
    void anUnsubscribedQueryFindsNoHandlerAnymore() throws Exception {
        String name = unique("Gone");
        Transport handler = transport(unique("handler"));
        Registration registration = channel(handler).registerQueryHandler(
                answering(query -> List.of(answer(query.messageId(), "here"))), name, "text", "client-gone", "svc");
        await(registration.ackFuture());
        QueryChannel dispatcher = channel(transport(unique("dispatcher")));
        gather(dispatcher, name, 1);

        registration.cancel();

        eventually("the query has no handler", 10, () -> {
            ResultStream<QueryResult> results = dispatcher.query(query(name, "?"));
            try {
                drain(results);
            } catch (Exception | AssertionError e) {
                return false;
            }
            return results.getError().filter(RequestFailedException.class::isInstance)
                    .map(RequestFailedException.class::cast)
                    .filter(e -> e.code() == ErrorCode.UNAVAILABLE).isPresent();
        });
    }

    // --------------------------------------------------- cancel and timeouts

    @Test
    void cancellingAQueryTellsTheHandlersStillWorkingOnIt() throws Exception {
        String name = unique("Slow");
        // A handler on the raw connection: it receives the query and never answers.
        Transport handler = transport(unique("handler"));
        List<Frame> frames = new CopyOnWriteArrayList<>();
        ClientStream stream = handler.connection().open(new OpenQueryHandlers(0, "client-slow-q", "svc"), false,
                new StreamListener() {
                    @Override
                    public void onFrame(Frame frame, boolean last) {
                        frames.add(frame);
                    }

                    @Override
                    public void onFailure(Throwable cause) {
                    }
                });
        stream.send(new SubscribeQuery("s-1", name, "text"));
        stream.send(new Credit(10));
        QueryChannel dispatcher = channel(transport(unique("dispatcher")));

        ResultStream<QueryResult> results = null;
        for (int attempt = 0; attempt < 100 && frames.stream().noneMatch(f -> f instanceof io.kronosdb.protocol.HandleQuery); attempt++) {
            results = dispatcher.query(query(name, "?"));
            Thread.sleep(100);
        }
        String requestId = frames.stream().filter(io.kronosdb.protocol.HandleQuery.class::isInstance)
                .map(f -> ((io.kronosdb.protocol.HandleQuery) f).query().messageId()).findFirst().orElseThrow();

        // Every attempt that reached the handler is still pending; give them all up.
        results.close();
        eventually("the handler hears of the cancel", 10, () -> frames.stream()
                .anyMatch(f -> f instanceof CancelQuery cancel && cancel.requestId().equals(requestId)));
    }

    @Test
    void aQueryItsHandlerNeverAnswersFailsAsUnavailableAtTheDeadline() throws Exception {
        RealServer.requireShortTimeouts();
        String name = unique("Hang");
        CompletableFuture<Void> received = new CompletableFuture<>();
        Transport handler = transport(unique("handler"));
        await(channel(handler).registerQueryHandler(new QueryChannel.QueryHandler() {
            @Override
            public void handle(Query query, Consumer<QueryResult> responseSender, Runnable onComplete) {
                received.complete(null); // neither answers nor completes
            }

            @Override
            public Registration registerSubscriptionQuery(io.kronosdb.protocol.SubscriptionQuery query,
                                                          QueryChannel.SubscriptionUpdateSender updateSender) {
                throw new UnsupportedOperationException();
            }
        }, name, "text", "client-hang-q", "svc").ackFuture());
        QueryChannel dispatcher = channel(transport(unique("dispatcher")));

        ResultStream<QueryResult> results = null;
        for (int attempt = 0; attempt < 100 && !received.isDone(); attempt++) {
            results = dispatcher.query(query(name, "?"));
            Thread.sleep(100);
        }
        ResultStream<QueryResult> last = results;
        eventually("the query ends", 15, last::isCompleted);

        assertThat(received).isCompleted();
        assertThat(last.getError()).hasValueSatisfying(error -> assertThat(error)
                .isInstanceOfSatisfying(RequestFailedException.class, e -> {
                    // The protocol has no deadline code; the message says which it was.
                    assertThat(e.code()).isEqualTo(ErrorCode.UNAVAILABLE);
                    assertThat(e.getMessage()).contains("timed out");
                }));
    }

    // ----------------------------------------------------------- recovery

    @Test
    void aQueryHandlerWhoseConnectionIsLostComesBackAndAnswersAgain() throws Exception {
        String name = unique("Phoenix");
        AtomicInteger calls = new AtomicInteger();
        Transport handler = transport(unique("handler"));
        await(channel(handler).registerQueryHandler(
                answering(query -> List.of(answer(query.messageId(), "call-" + calls.incrementAndGet()))),
                name, "text", "client-phoenix", "svc").ackFuture());
        QueryChannel dispatcher = channel(transport(unique("dispatcher")));
        assertThat(gather(dispatcher, name, 1)).hasSize(1);
        int before = calls.get();

        handler.connection().close();

        // The channel opens a new handler stream on a new connection and
        // subscribes again.
        eventually("the handler answers again", 20, () -> {
            try {
                return !drain(dispatcher.query(query(name, "?"))).isEmpty() && calls.get() > before;
            } catch (Exception | AssertionError e) {
                return false;
            }
        });
    }

    @Test
    void aQueryWithoutANameIsRejectedAsInvalid() throws Exception {
        QueryChannel dispatcher = channel(transport(unique("dispatcher")));
        ResultStream<QueryResult> results = dispatcher.query(query("", "?"));

        eventually("the query fails", 10, results::isCompleted);
        assertThat(results.getError()).hasValueSatisfying(error -> assertThat(error)
                .isInstanceOfSatisfying(RequestFailedException.class,
                        e -> assertThat(e.code()).isEqualTo(ErrorCode.INVALID_ARGUMENT)));
    }
}
