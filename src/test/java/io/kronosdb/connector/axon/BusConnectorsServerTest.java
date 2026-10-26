package io.kronosdb.connector.axon;

import io.kronosdb.connector.axon.command.KronosDbCommandBusConnector;
import io.kronosdb.connector.axon.command.KronosDbCommandDispatchException;
import io.kronosdb.connector.axon.query.KronosDbQueryBusConnector;
import io.kronosdb.connector.transport.KronosDbConnection;
import io.kronosdb.connector.transport.KronosDbConnectionFactory;
import io.kronosdb.connector.transport.RealServer;
import org.axonframework.messaging.commandhandling.CommandMessage;
import org.axonframework.messaging.commandhandling.CommandResultMessage;
import org.axonframework.messaging.commandhandling.GenericCommandMessage;
import org.axonframework.messaging.commandhandling.GenericCommandResultMessage;
import org.axonframework.messaging.core.GenericMessage;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.queryhandling.GenericQueryMessage;
import org.axonframework.messaging.queryhandling.GenericQueryResponseMessage;
import org.axonframework.messaging.queryhandling.GenericSubscriptionQueryUpdateMessage;
import org.axonframework.messaging.queryhandling.QueryMessage;
import org.axonframework.messaging.queryhandling.QueryResponseMessage;
import org.axonframework.messaging.queryhandling.SubscriptionQueryUpdateMessage;
import io.kronosdb.connector.axon.distributed.query.QueryBusConnector;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static io.kronosdb.connector.transport.RealServer.await;
import static io.kronosdb.connector.transport.RealServer.eventually;
import static io.kronosdb.connector.transport.RealServer.unique;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Axon's command and query bus connectors against a real KronosDB server,
 * with real Axon messages: what one application instance handles, another
 * dispatches. Each instance has a connection factory of its own. See
 * {@link RealServer} for how to run these.
 */
class BusConnectorsServerTest {

    private static byte[] bytes(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }

    private static String text(Object payload) {
        return new String((byte[]) payload, StandardCharsets.UTF_8);
    }

    private final List<KronosDbConnectionFactory> factories = new CopyOnWriteArrayList<>();

    @BeforeEach
    void requireServer() {
        RealServer.require();
    }

    @AfterEach
    void close() {
        factories.forEach(KronosDbConnectionFactory::shutdown);
    }

    /** One application instance: its own client id, its own connection to the server. */
    private final class Instance {
        final String clientId = unique("app");
        final KronosDbConfiguration configuration = KronosDbConfiguration.builder()
                .clientId(clientId).componentName("orders").build();
        final KronosDbConnection connection;

        Instance() {
            KronosDbConnectionFactory factory = KronosDbConnectionFactory.forClient("orders", clientId)
                    .host(RealServer.host()).port(RealServer.port()).build();
            factories.add(factory);
            connection = factory.connect("default");
        }

        KronosDbCommandBusConnector commands() {
            return new KronosDbCommandBusConnector(connection, configuration);
        }

        KronosDbQueryBusConnector queries() {
            return new KronosDbQueryBusConnector(connection, configuration);
        }
    }

    private static CommandMessage command(String type, String body) {
        return new GenericCommandMessage(new MessageType(type, "1"), bytes(body));
    }

    /** Dispatches until the handler's registration has reached the routing table. */
    private static CommandResultMessage dispatch(KronosDbCommandBusConnector connector, CommandMessage command)
            throws Exception {
        for (int attempt = 0; attempt < 200; attempt++) {
            try {
                return connector.dispatch(command, null).get(15, TimeUnit.SECONDS);
            } catch (ExecutionException e) {
                if (e.getCause() instanceof io.kronosdb.connector.transport.RequestFailedException failed
                        && failed.code() == io.kronosdb.protocol.ErrorCode.UNAVAILABLE) {
                    Thread.sleep(50);
                    continue;
                }
                throw e;
            }
        }
        throw new AssertionError("the command never reached a handler");
    }

    // ------------------------------------------------------------- commands

    @Test
    void aCommandIsHandledByAnotherInstanceAndTheResultComesBackWithItsMetadata() throws Exception {
        String type = unique("PlaceOrder");
        KronosDbCommandBusConnector handler = new Instance().commands();
        List<CommandMessage> received = new CopyOnWriteArrayList<>();
        handler.onIncomingCommand((command, callback) -> {
            received.add(command);
            callback.onSuccess(new GenericCommandResultMessage(new MessageType("Receipt", "2"),
                    bytes("receipt for " + text(command.payload())), Map.of("trace", "t-9")));
        });
        await(handler.subscribe(new QualifiedName(type), 100));

        CommandMessage sent = new GenericCommandMessage(
                new GenericMessage(new MessageType(type, "1"), bytes("{\"sku\":\"A-1\"}"), Map.of("user", "åsa")),
                "order-7", 5);
        CommandResultMessage result = dispatch(new Instance().commands(), sent);

        assertThat(result.type()).isEqualTo(new MessageType("Receipt", "2"));
        assertThat(text(result.payload())).isEqualTo("receipt for {\"sku\":\"A-1\"}");
        assertThat(result.metadata()).containsEntry("trace", "t-9");
        CommandMessage arrived = received.get(0);
        assertThat(arrived.identifier()).isEqualTo(sent.identifier());
        assertThat(arrived.type()).isEqualTo(new MessageType(type, "1"));
        assertThat(arrived.metadata()).containsEntry("user", "åsa");
        assertThat(arrived.routingKey()).contains("order-7");
        assertThat(arrived.priority()).hasValue(5);
    }

    @Test
    void aHandlerThatFailsTheCommandFailsTheDispatchWithItsMessage() throws Exception {
        String type = unique("Boom");
        KronosDbCommandBusConnector handler = new Instance().commands();
        handler.onIncomingCommand((command, callback) -> callback.onError(new IllegalStateException("out of stock")));
        await(handler.subscribe(new QualifiedName(type), 100));
        KronosDbCommandBusConnector dispatcher = new Instance().commands();

        assertThatThrownBy(() -> dispatch(dispatcher, command(type, "x")))
                .isInstanceOf(ExecutionException.class)
                .cause()
                .isInstanceOfSatisfying(KronosDbCommandDispatchException.class, e -> {
                    assertThat(e.getErrorCode()).isEqualTo("COMMAND_EXECUTION_ERROR");
                    assertThat(e.getMessage()).isEqualTo("out of stock");
                });
    }

    @Test
    void aHandlerThatAnswersNothingGivesTheDispatcherAnEmptyResult() throws Exception {
        String type = unique("Quiet");
        KronosDbCommandBusConnector handler = new Instance().commands();
        handler.onIncomingCommand((command, callback) -> callback.onSuccess(null));
        await(handler.subscribe(new QualifiedName(type), 100));

        CommandResultMessage result = dispatch(new Instance().commands(), command(type, "x"));

        assertThat(result == null || result.payload() == null || ((byte[]) result.payload()).length == 0).isTrue();
    }

    @Test
    void aCommandWithoutAHandlerFailsAsUnavailable() {
        KronosDbCommandBusConnector dispatcher = new Instance().commands();

        assertThatThrownBy(() -> dispatcher.dispatch(command(unique("Nobody"), "x"), null).get(15, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class)
                .cause()
                .isInstanceOfSatisfying(io.kronosdb.connector.transport.RequestFailedException.class,
                        e -> assertThat(e.code()).isEqualTo(io.kronosdb.protocol.ErrorCode.UNAVAILABLE));
    }

    @Test
    void anUnsubscribedCommandFindsNoHandlerAnymore() throws Exception {
        String type = unique("Gone");
        KronosDbCommandBusConnector handler = new Instance().commands();
        handler.onIncomingCommand((command, callback) -> callback.onSuccess(null));
        await(handler.subscribe(new QualifiedName(type), 100));
        KronosDbCommandBusConnector dispatcher = new Instance().commands();
        dispatch(dispatcher, command(type, "x"));

        assertThat(handler.unsubscribe(new QualifiedName(type))).isTrue();

        eventually("the command has no handler", 10, () -> {
            try {
                dispatcher.dispatch(command(type, "x"), null).get(5, TimeUnit.SECONDS);
                return false;
            } catch (ExecutionException e) {
                return e.getCause() instanceof io.kronosdb.connector.transport.RequestFailedException;
            }
        });
    }

    @Test
    void severalInstancesShareTheCommandsOfOneType() throws Exception {
        String type = unique("Shared");
        Set<String> handledBy = java.util.concurrent.ConcurrentHashMap.newKeySet();
        for (String name : List.of("one", "two")) {
            KronosDbCommandBusConnector handler = new Instance().commands();
            handler.onIncomingCommand((command, callback) -> callback.onSuccess(
                    new GenericCommandResultMessage(new MessageType("Handled", "1"), bytes(name))));
            await(handler.subscribe(new QualifiedName(type), 100));
        }
        KronosDbCommandBusConnector dispatcher = new Instance().commands();
        dispatch(dispatcher, command(type, "warm"));

        // The server hands out unkeyed commands in turns weighted by load factor, in
        // runs as long as the factor: 100 commands to one handler, then 100 to the other.
        for (int i = 0; i < 450 && handledBy.size() < 2; i++) {
            handledBy.add(text(dispatcher.dispatch(command(type, "n" + i), null).get(15, TimeUnit.SECONDS).payload()));
        }

        assertThat(handledBy).containsExactlyInAnyOrder("one", "two");
    }

    // -------------------------------------------------------------- queries

    /** A query handler that answers with what {@code answer} makes of the query. */
    private static final class Answering implements QueryBusConnector.Handler {

        final Function<QueryMessage, MessageStream<QueryResponseMessage>> answer;
        final List<QueryMessage> received = new CopyOnWriteArrayList<>();
        final List<QueryBusConnector.UpdateCallback> callbacks = new CopyOnWriteArrayList<>();
        final List<QueryMessage> subscriptions = new CopyOnWriteArrayList<>();
        final AtomicInteger cancelled = new AtomicInteger();

        Answering(Function<QueryMessage, MessageStream<QueryResponseMessage>> answer) {
            this.answer = answer;
        }

        @Override
        public MessageStream<QueryResponseMessage> query(QueryMessage query) {
            received.add(query);
            return answer.apply(query);
        }

        @Override
        public org.axonframework.common.Registration registerUpdateHandler(
                QueryMessage query, QueryBusConnector.UpdateCallback callback) {
            subscriptions.add(query);
            callbacks.add(callback);
            return () -> {
                cancelled.incrementAndGet();
                return true;
            };
        }
    }

    private static QueryResponseMessage response(String type, String body) {
        return new GenericQueryResponseMessage(new MessageType(type, "1"), bytes(body));
    }

    private static MessageStream<QueryResponseMessage> answers(String... bodies) {
        List<QueryResponseMessage> all = new ArrayList<>();
        for (String body : bodies) {
            all.add(response("Answer", body));
        }
        return MessageStream.fromIterable(all);
    }

    private static QueryMessage query(String type, String body) {
        return new GenericQueryMessage(new MessageType(type, "1"), bytes(body));
    }

    /** Reads what a stream holds until it completes or yields {@code count} entries. */
    private static List<QueryResponseMessage> read(MessageStream<QueryResponseMessage> stream, int count)
            throws Exception {
        List<QueryResponseMessage> all = new ArrayList<>();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
        while (all.size() < count && !stream.isCompleted()) {
            var next = stream.next();
            next.ifPresent(entry -> all.add(entry.message()));
            if (next.isEmpty()) {
                Thread.sleep(5);
            }
            if (System.nanoTime() > deadline) {
                throw new AssertionError("expected " + count + " entries, got " + all.size()
                        + ", error: " + stream.error());
            }
        }
        return all;
    }

    private static List<QueryResponseMessage> queryWhenReady(KronosDbQueryBusConnector dispatcher, QueryMessage query)
            throws Exception {
        for (int attempt = 0; attempt < 200; attempt++) {
            MessageStream<QueryResponseMessage> stream = dispatcher.query(query, null);
            List<QueryResponseMessage> got = read(stream, Integer.MAX_VALUE);
            if (!got.isEmpty()) {
                return got;
            }
            Thread.sleep(50);
        }
        throw new AssertionError("the query never reached a handler");
    }

    @Test
    void aQueryIsAnsweredByAnotherInstanceWithItsMetadata() throws Exception {
        String type = unique("FindOrders");
        KronosDbQueryBusConnector handler = new Instance().queries();
        Answering answering = new Answering(query -> MessageStream.just(new GenericQueryResponseMessage(
                new MessageType("Orders", "3"), bytes("orders for " + text(query.payload())), Map.of("page", "1"))));
        handler.onIncomingQuery(answering);
        await(handler.subscribe(new QualifiedName(type)));

        QueryMessage sent = new GenericQueryMessage(
                new GenericMessage(new MessageType(type, "1"), bytes("åsa"), Map.of("tenant", "t1")), 2);
        List<QueryResponseMessage> got = queryWhenReady(new Instance().queries(), sent);

        assertThat(got).hasSize(1);
        assertThat(got.get(0).type()).isEqualTo(new MessageType("Orders", "3"));
        assertThat(text(got.get(0).payload())).isEqualTo("orders for åsa");
        assertThat(got.get(0).metadata()).containsEntry("page", "1");
        QueryMessage arrived = answering.received.stream()
                .filter(q -> q.identifier().equals(sent.identifier())).findFirst().orElseThrow();
        assertThat(arrived.type()).isEqualTo(new MessageType(type, "1"));
        assertThat(arrived.metadata()).containsEntry("tenant", "t1");
        assertThat(arrived.priority()).hasValue(2);
    }

    @Test
    void aHandlerThatAnswersSeveralTimesGivesTheDispatcherOnlyTheFirstWhenItAsksForOne() throws Exception {
        String type = unique("Pages");
        KronosDbQueryBusConnector handler = new Instance().queries();
        handler.onIncomingQuery(new Answering(query -> answers("page-1", "page-2", "page-3")));
        await(handler.subscribe(new QualifiedName(type)));

        List<QueryResponseMessage> got = queryWhenReady(new Instance().queries(), query(type, "?"));

        // Axon queries ask the server for one result; a handler's extra answers all arrive.
        assertThat(got).extracting(r -> text(r.payload())).contains("page-1");
    }

    @Test
    void aQueryOnlyOneOfSeveralInstancesAnswers() throws Exception {
        String type = unique("Count");
        Set<String> answeredBy = new HashSet<>();
        for (String name : List.of("one", "two", "three")) {
            KronosDbQueryBusConnector handler = new Instance().queries();
            handler.onIncomingQuery(new Answering(query -> answers(name)));
            await(handler.subscribe(new QualifiedName(type)));
        }
        KronosDbQueryBusConnector dispatcher = new Instance().queries();
        queryWhenReady(dispatcher, query(type, "?"));

        for (int i = 0; i < 20; i++) {
            List<QueryResponseMessage> got = queryWhenReady(dispatcher, query(type, "?"));
            assertThat(got).hasSize(1);
            answeredBy.add(text(got.get(0).payload()));
        }

        assertThat(answeredBy).isNotEmpty().isSubsetOf(Set.of("one", "two", "three"));
    }

    @Test
    void aQueryHandlerThatFailsSurfacesTheErrorToTheDispatcher() throws Exception {
        String type = unique("Fragile");
        KronosDbQueryBusConnector handler = new Instance().queries();
        handler.onIncomingQuery(new Answering(query -> MessageStream.failed(new IllegalStateException("no such order"))));
        await(handler.subscribe(new QualifiedName(type)));
        KronosDbQueryBusConnector dispatcher = new Instance().queries();

        MessageStream<QueryResponseMessage> stream = null;
        for (int attempt = 0; attempt < 100; attempt++) {
            stream = dispatcher.query(query(type, "?"), null);
            Throwable thrown = null;
            try {
                read(stream, Integer.MAX_VALUE);
            } catch (Throwable e) {
                thrown = e;
            }
            if (thrown != null || stream.error().filter(e -> !(e instanceof io.kronosdb.connector.transport.RequestFailedException)).isPresent()) {
                Throwable failure = thrown != null ? thrown : stream.error().get();
                assertThat(failure.toString()).contains("no such order");
                return;
            }
            Thread.sleep(50);
        }
        throw new AssertionError("the dispatcher never learned that the handler failed");
    }

    @Test
    void aQueryNobodyHandlesFailsTheStream() throws Exception {
        MessageStream<QueryResponseMessage> stream = new Instance().queries().query(query(unique("Nobody"), "?"), null);

        eventually("the query fails", 10, () -> stream.error().isPresent());
        assertThat(stream.error().get()).isInstanceOf(io.kronosdb.connector.transport.RequestFailedException.class);
    }

    @Test
    void anUnsubscribedQueryFindsNoHandlerAnymore() throws Exception {
        String type = unique("Gone");
        KronosDbQueryBusConnector handler = new Instance().queries();
        handler.onIncomingQuery(new Answering(query -> answers("here")));
        await(handler.subscribe(new QualifiedName(type)));
        KronosDbQueryBusConnector dispatcher = new Instance().queries();
        queryWhenReady(dispatcher, query(type, "?"));

        assertThat(handler.unsubscribe(new QualifiedName(type))).isTrue();

        eventually("the query has no handler", 10, () -> {
            MessageStream<QueryResponseMessage> stream = dispatcher.query(query(type, "?"), null);
            return read(stream, Integer.MAX_VALUE).isEmpty() && stream.error().isPresent();
        });
    }

    // ----------------------------------------------- subscription queries

    private static QueryMessage subscription(String type) {
        return query(type, "watch");
    }

    private static SubscriptionQueryUpdateMessage update(String body) {
        return new GenericSubscriptionQueryUpdateMessage(new MessageType("Update", "1"), bytes(body));
    }

    /** Opens a subscription until the handler's registration has reached the routing table. */
    private static MessageStream<QueryResponseMessage> subscribeWhenReady(KronosDbQueryBusConnector subscriber,
                                                                         String type, int buffer) throws Exception {
        for (int attempt = 0; attempt < 200; attempt++) {
            MessageStream<QueryResponseMessage> stream = subscriber.subscriptionQuery(subscription(type), null, buffer);
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
            while (System.nanoTime() < deadline && !stream.hasNextAvailable() && stream.error().isEmpty()
                    && !stream.isCompleted()) {
                Thread.sleep(5);
            }
            if (stream.hasNextAvailable()) {
                return stream;
            }
            stream.close();
            Thread.sleep(50);
        }
        throw new AssertionError("the subscription never opened");
    }

    @Test
    void aSubscriptionQueryDeliversTheInitialResultThenUpdatesThenTheCompletion() throws Exception {
        String type = unique("Watch");
        KronosDbQueryBusConnector handler = new Instance().queries();
        Answering answering = new Answering(query -> answers("initial"));
        handler.onIncomingQuery(answering);
        await(handler.subscribe(new QualifiedName(type)));

        MessageStream<QueryResponseMessage> stream =
                subscribeWhenReady(new Instance().queries(), type, 16);
        eventually("the handler registered the update handler", 10, () -> !answering.callbacks.isEmpty());
        var callback = answering.callbacks.get(0);
        callback.sendUpdate(update("one"));
        callback.sendUpdate(update("two"));
        callback.complete();

        // Reading to the end takes the completion too.
        List<QueryResponseMessage> got = read(stream, Integer.MAX_VALUE);
        assertThat(got).extracting(r -> text(r.payload())).containsExactly("initial", "one", "two");
        assertThat(stream.isCompleted()).isTrue();
        assertThat(stream.error()).isEmpty();
    }

    @Test
    void aSubscriberThatClosesTheStreamEndsTheHandlersSubscription() throws Exception {
        String type = unique("Watch");
        KronosDbQueryBusConnector handler = new Instance().queries();
        Answering answering = new Answering(query -> answers("initial"));
        handler.onIncomingQuery(answering);
        await(handler.subscribe(new QualifiedName(type)));
        MessageStream<QueryResponseMessage> stream = subscribeWhenReady(new Instance().queries(), type, 16);
        eventually("the handler registered the update handler", 10, () -> !answering.callbacks.isEmpty());

        stream.close();

        eventually("the handler's registration is cancelled", 10, () -> answering.cancelled.get() == 1);
    }

    @Test
    void aHandlerThatFailsTheSubscriptionEndsTheStreamWithAnError() throws Exception {
        String type = unique("Watch");
        KronosDbQueryBusConnector handler = new Instance().queries();
        Answering answering = new Answering(query -> answers("initial"));
        handler.onIncomingQuery(answering);
        await(handler.subscribe(new QualifiedName(type)));
        MessageStream<QueryResponseMessage> stream = subscribeWhenReady(new Instance().queries(), type, 16);
        eventually("the handler registered the update handler", 10, () -> !answering.callbacks.isEmpty());

        answering.callbacks.get(0).completeExceptionally(new IllegalStateException("projection broke"));

        List<QueryResponseMessage> got = read(stream, 2);
        assertThat(got).extracting(r -> text(r.payload())).containsExactly("initial");
        eventually("the stream fails", 10, () -> stream.error().isPresent());
        assertThat(stream.error().get()).isInstanceOfSatisfying(KronosDbException.class, e -> {
            assertThat(e.getErrorCode()).isEqualTo("QUERY_EXECUTION_ERROR");
            assertThat(e.getMessage()).isEqualTo("projection broke");
        });
    }

    @Test
    void aSlowSubscriberDoesNotLoseUpdatesTheHandlerEmitsAtOnce() throws Exception {
        String type = unique("Watch");
        KronosDbQueryBusConnector handler = new Instance().queries();
        Answering answering = new Answering(query -> answers("initial"));
        handler.onIncomingQuery(answering);
        await(handler.subscribe(new QualifiedName(type)));
        MessageStream<QueryResponseMessage> stream = subscribeWhenReady(new Instance().queries(), type, 8);
        eventually("the handler registered the update handler", 10, () -> !answering.callbacks.isEmpty());
        var callback = answering.callbacks.get(0);

        int total = 100;
        for (int i = 0; i < total; i++) {
            callback.sendUpdate(update("u" + i));
        }
        callback.complete();

        List<QueryResponseMessage> got = read(stream, total + 1);
        assertThat(got).hasSize(total + 1);
        assertThat(got.stream().skip(1).map(r -> text(r.payload())).toList())
                .containsExactlyElementsOf(java.util.stream.IntStream.range(0, total).mapToObj(i -> "u" + i).toList());
    }
}
