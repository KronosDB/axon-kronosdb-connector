package io.kronosdb.connector.axon;

import io.kronosdb.connector.axon.command.KronosDbCommandBusConnector;
import io.kronosdb.connector.axon.command.KronosDbCommandDispatchException;
import io.kronosdb.connector.axon.query.KronosDbQueryBusConnector;
import io.kronosdb.connector.transport.FakeServer;
import io.kronosdb.connector.transport.KronosDbConnection;
import io.kronosdb.connector.transport.KronosDbConnectionFactory;
import io.kronosdb.protocol.Ack;
import io.kronosdb.protocol.CommandHandled;
import io.kronosdb.protocol.CommandResponse;
import io.kronosdb.protocol.CommandResult;
import io.kronosdb.protocol.Credit;
import io.kronosdb.protocol.DispatchCommand;
import io.kronosdb.protocol.DispatchQuery;
import io.kronosdb.protocol.ErrorDetail;
import io.kronosdb.protocol.HandleCommand;
import io.kronosdb.protocol.HandleQuery;
import io.kronosdb.protocol.MessageMetadata;
import io.kronosdb.protocol.MetadataValue;
import io.kronosdb.protocol.OpenCommandHandlers;
import io.kronosdb.protocol.OpenQueryHandlers;
import io.kronosdb.protocol.Payload;
import io.kronosdb.protocol.ProcessingInstruction;
import io.kronosdb.protocol.ProcessingKey;
import io.kronosdb.protocol.QueryDone;
import io.kronosdb.protocol.QueryEnd;
import io.kronosdb.protocol.QueryHandled;
import io.kronosdb.protocol.QueryResponse;
import io.kronosdb.protocol.QueryResult;
import io.kronosdb.protocol.ServerAck;
import io.kronosdb.protocol.SubscribeCommand;
import io.kronosdb.protocol.SubscribeQuery;
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
import org.axonframework.messaging.queryhandling.QueryMessage;
import org.axonframework.messaging.queryhandling.QueryResponseMessage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Axon's command and query bus connectors on a scripted server: what they put
 * on the wire for Axon messages and what they make of the frames they get
 * back. As with {@code MessagingChannelsTest}, the server is a stand-in written
 * from the schema, not the real KronosDB.
 */
class BusConnectorsTest {

    private static final byte[] BODY = "{\"sku\":\"A-1\"}".getBytes(StandardCharsets.UTF_8);

    private final List<AutoCloseable> closeables = new CopyOnWriteArrayList<>();

    @AfterEach
    void close() throws Exception {
        for (AutoCloseable c : closeables) {
            c.close();
        }
    }

    @FunctionalInterface
    interface Script {
        void run(FakeServer.Session session) throws Exception;
    }

    private KronosDbConnection connection(Script script) throws IOException {
        Consumer<FakeServer.Session> wrapped = session -> {
            try {
                session.handshake();
                script.run(session);
            } catch (IOException ignored) {
                // The client hung up.
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
        FakeServer server = new FakeServer(wrapped);
        closeables.add(server);
        KronosDbConnectionFactory factory = KronosDbConnectionFactory.forClient("orders", "client-1")
                .host("127.0.0.1")
                .port(server.port())
                .build();
        closeables.add(factory::shutdown);
        return factory.connect("default");
    }

    private static KronosDbConfiguration configuration() {
        return KronosDbConfiguration.builder().clientId("client-1").componentName("orders").build();
    }

    private static <T> T await(CompletableFuture<T> f) throws Exception {
        return f.get(10, TimeUnit.SECONDS);
    }

    private static Payload payload(String type, String revision, byte[] data) {
        return new Payload(type, revision, ByteBuffer.wrap(data));
    }

    private static byte[] bytes(ByteBuffer buffer) {
        byte[] out = new byte[buffer.remaining()];
        buffer.get(buffer.position(), out);
        return out;
    }

    // ------------------------------------------------------------- commands

    @Test
    void dispatchingACommandPutsItsTypeRoutingKeyPriorityAndMetadataOnTheWire() throws Exception {
        CompletableFuture<DispatchCommand> seen = new CompletableFuture<>();
        KronosDbConnection connection = connection(session -> {
            FakeServer.Session.Incoming in = session.next(DispatchCommand.class);
            DispatchCommand dispatch = (DispatchCommand) in.frame();
            seen.complete(dispatch);
            session.reply(in.stream(), new CommandResponse(new CommandResult(
                    "res-1", dispatch.command().messageId(), "", null,
                    payload("Receipt", "2", "ok".getBytes(StandardCharsets.UTF_8)),
                    List.of(new MessageMetadata("trace", new MetadataValue.OfText("t-9")),
                            new MessageMetadata("attempts", new MetadataValue.OfNumber(3))),
                    List.of())));
        });
        KronosDbCommandBusConnector connector = new KronosDbCommandBusConnector(connection, configuration());

        CommandMessage command = new GenericCommandMessage(
                new GenericMessage(new MessageType("PlaceOrder", "1"), BODY, Map.of("user", "åsa")), "order-7", 5);
        CommandResultMessage result = await(connector.dispatch(command, null));

        io.kronosdb.protocol.Command wire = (await(seen)).command();
        assertThat(wire.messageId()).isEqualTo(command.identifier());
        assertThat(wire.name()).isEqualTo("PlaceOrder");
        assertThat(wire.payload().typeName()).isEqualTo("PlaceOrder");
        assertThat(wire.payload().revision()).isEqualTo("1");
        assertThat(bytes(wire.payload().data())).isEqualTo(BODY);
        assertThat(wire.metadata()).containsExactly(new MessageMetadata("user", new MetadataValue.OfText("åsa")));
        assertThat(wire.instructions()).containsExactlyInAnyOrder(
                new ProcessingInstruction(ProcessingKey.ROUTING_KEY, new MetadataValue.OfText("order-7")),
                new ProcessingInstruction(ProcessingKey.PRIORITY, new MetadataValue.OfNumber(5)));
        assertThat(wire.clientId()).isEqualTo("client-1");
        assertThat(wire.componentName()).isEqualTo("orders");

        assertThat(result.type()).isEqualTo(new MessageType("Receipt", "2"));
        assertThat((byte[]) result.payload()).isEqualTo("ok".getBytes(StandardCharsets.UTF_8));
        assertThat(result.metadata()).containsEntry("trace", "t-9").containsEntry("attempts", "3");
    }

    @Test
    void aCommandTheHandlerRefusedFailsTheDispatchWithItsCodeAndMessage() throws Exception {
        KronosDbConnection connection = connection(session -> {
            FakeServer.Session.Incoming in = session.next(DispatchCommand.class);
            session.reply(in.stream(), new CommandResponse(new CommandResult(
                    "res-1", ((DispatchCommand) in.frame()).command().messageId(), "OUT_OF_STOCK",
                    new ErrorDetail("A-1 is out of stock", "warehouse", List.of(), "OUT_OF_STOCK"),
                    null, List.of(), List.of())));
        });
        KronosDbCommandBusConnector connector = new KronosDbCommandBusConnector(connection, configuration());
        CommandMessage command = new GenericCommandMessage(new MessageType("PlaceOrder", "1"), BODY);

        assertThatThrownBy(() -> await(connector.dispatch(command, null)))
                .isInstanceOf(ExecutionException.class)
                .cause()
                .isInstanceOfSatisfying(KronosDbCommandDispatchException.class, e -> {
                    assertThat(e.getErrorCode()).isEqualTo("OUT_OF_STOCK");
                    assertThat(e.getMessage()).isEqualTo("A-1 is out of stock");
                });
    }

    @Test
    void aCommandForTheLocalHandlerReachesAxonAndTheResultGoesBack() throws Exception {
        CompletableFuture<CommandHandled> handled = new CompletableFuture<>();
        KronosDbConnection connection = connection(session -> {
            FakeServer.Session.Incoming open = session.next(OpenCommandHandlers.class);
            session.next(Credit.class);
            SubscribeCommand subscribe = (SubscribeCommand) session.next(SubscribeCommand.class).frame();
            session.send(open.stream(), 0, new ServerAck(new Ack(subscribe.instructionId(), true, null)));
            session.send(open.stream(), 0, new HandleCommand(new io.kronosdb.protocol.Command(
                    "c-1", "PlaceOrder", 1000, payload("PlaceOrder", "1", BODY),
                    List.of(new MessageMetadata("user", new MetadataValue.OfText("åsa"))),
                    List.of(new ProcessingInstruction(ProcessingKey.ROUTING_KEY, new MetadataValue.OfText("order-7")),
                            new ProcessingInstruction(ProcessingKey.PRIORITY, new MetadataValue.OfNumber(5))),
                    "caller", "web")));
            handled.complete((CommandHandled) session.next(CommandHandled.class).frame());
            session.next();
        });
        KronosDbCommandBusConnector connector = new KronosDbCommandBusConnector(connection, configuration());
        CompletableFuture<CommandMessage> received = new CompletableFuture<>();
        connector.onIncomingCommand((command, callback) -> {
            received.complete(command);
            callback.onSuccess(new GenericCommandResultMessage(new MessageType("Receipt", "1"),
                    "done".getBytes(StandardCharsets.UTF_8), Map.of("k", "v")));
        });

        await(connector.subscribe(new QualifiedName("PlaceOrder"), 2));

        CommandMessage command = await(received);
        assertThat(command.identifier()).isEqualTo("c-1");
        assertThat(command.type()).isEqualTo(new MessageType("PlaceOrder", "1"));
        assertThat((byte[]) command.payload()).isEqualTo(BODY);
        assertThat(command.metadata()).containsEntry("user", "åsa");
        assertThat(command.routingKey()).contains("order-7");
        assertThat(command.priority()).hasValue(5);

        CommandResult result = await(handled).result();
        assertThat(result.requestId()).isEqualTo("c-1");
        assertThat(result.errorCode()).isEmpty();
        assertThat(result.payload().typeName()).isEqualTo("Receipt");
        assertThat(bytes(result.payload().data())).isEqualTo("done".getBytes(StandardCharsets.UTF_8));
        assertThat(result.metadata()).containsExactly(new MessageMetadata("k", new MetadataValue.OfText("v")));
    }

    // -------------------------------------------------------------- queries

    @Test
    void queryingAsksForOneResultAndReadsTheAnswersUntilTheEnd() throws Exception {
        CompletableFuture<DispatchQuery> seen = new CompletableFuture<>();
        KronosDbConnection connection = connection(session -> {
            FakeServer.Session.Incoming in = session.next(DispatchQuery.class);
            DispatchQuery dispatch = (DispatchQuery) in.frame();
            seen.complete(dispatch);
            session.send(in.stream(), 0, new QueryResponse(new QueryResult("a", dispatch.query().messageId(), "",
                    null, payload("Orders", "1", "first".getBytes(StandardCharsets.UTF_8)),
                    List.of(new MessageMetadata("n", new MetadataValue.OfDouble(0.5))), List.of())));
            session.reply(in.stream(), new QueryEnd());
            session.next();
        });
        KronosDbQueryBusConnector connector = new KronosDbQueryBusConnector(connection, configuration());

        QueryMessage query = new GenericQueryMessage(new GenericMessage(new MessageType("FindOrders", "1"), BODY,
                Map.of("tenant", "t1")), 2);
        MessageStream<QueryResponseMessage> responses = connector.query(query, null);

        List<QueryResponseMessage> got = new ArrayList<>();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (!responses.isCompleted() && System.nanoTime() < deadline) {
            Optional<MessageStream.Entry<QueryResponseMessage>> next = responses.next();
            next.ifPresent(e -> got.add(e.message()));
            if (next.isEmpty()) {
                Thread.sleep(5);
            }
        }
        io.kronosdb.protocol.Query wire = await(seen).query();
        assertThat(wire.name()).isEqualTo("FindOrders");
        assertThat(wire.payload().typeName()).isEqualTo("FindOrders");
        assertThat(wire.metadata()).containsExactly(new MessageMetadata("tenant", new MetadataValue.OfText("t1")));
        assertThat(wire.instructions()).contains(
                new ProcessingInstruction(ProcessingKey.PRIORITY, new MetadataValue.OfNumber(2)),
                new ProcessingInstruction(ProcessingKey.NR_OF_RESULTS, new MetadataValue.OfNumber(1)));
        assertThat(got).hasSize(1);
        assertThat((byte[]) got.get(0).payload()).isEqualTo("first".getBytes(StandardCharsets.UTF_8));
        assertThat(got.get(0).metadata()).containsEntry("n", "0.5");
    }

    @Test
    void aQueryForTheLocalHandlerReachesAxonAndEachAnswerGoesBackBeforeDone() throws Exception {
        List<Object> sent = new CopyOnWriteArrayList<>();
        CompletableFuture<Void> done = new CompletableFuture<>();
        KronosDbConnection connection = connection(session -> {
            FakeServer.Session.Incoming open = session.next(OpenQueryHandlers.class);
            session.next(Credit.class);
            SubscribeQuery subscribe = (SubscribeQuery) session.next(SubscribeQuery.class).frame();
            sent.add(subscribe);
            session.send(open.stream(), 0, new ServerAck(new Ack(subscribe.instructionId(), true, null)));
            session.send(open.stream(), 0, new HandleQuery(new io.kronosdb.protocol.Query(
                    "q-1", "FindOrders", 1000, payload("FindOrders", "1", BODY),
                    List.of(), List.of(), "caller", "web")));
            sent.add(session.next(QueryHandled.class).frame());
            sent.add(session.next(QueryDone.class).frame());
            done.complete(null);
            session.next();
        });
        KronosDbQueryBusConnector connector = new KronosDbQueryBusConnector(connection, configuration());
        CompletableFuture<QueryMessage> received = new CompletableFuture<>();
        connector.onIncomingQuery(new io.kronosdb.connector.axon.distributed.query.QueryBusConnector.Handler() {
            @Override
            public MessageStream<QueryResponseMessage> query(QueryMessage query) {
                received.complete(query);
                return MessageStream.just(new GenericQueryResponseMessage(new MessageType("Orders", "1"),
                        "answer".getBytes(StandardCharsets.UTF_8)));
            }

            @Override
            public org.axonframework.common.Registration registerUpdateHandler(
                    QueryMessage query,
                    io.kronosdb.connector.axon.distributed.query.QueryBusConnector.UpdateCallback callback) {
                throw new AssertionError();
            }
        });

        await(connector.subscribe(new QualifiedName("FindOrders")));
        await(done);

        QueryMessage query = await(received);
        assertThat(query.identifier()).isEqualTo("q-1");
        assertThat(query.type().name()).isEqualTo("FindOrders");
        assertThat((byte[]) query.payload()).isEqualTo(BODY);
        assertThat(((SubscribeQuery) sent.get(0)).query()).isEqualTo("FindOrders");
        QueryResult answer = ((QueryHandled) sent.get(1)).result();
        assertThat(answer.requestId()).isEqualTo("q-1");
        assertThat(answer.payload().typeName()).isEqualTo("Orders");
        assertThat(bytes(answer.payload().data())).isEqualTo("answer".getBytes(StandardCharsets.UTF_8));
        assertThat(((QueryDone) sent.get(2)).requestId()).isEqualTo("q-1");
    }

    // ------------------------------------------------------------- metadata

    @Test
    void metadataOfEveryTypeBecomesTextAndObjectsAreDropped() {
        Map<String, String> text = MetadataConverter.fromWireMetadata(List.of(
                new MessageMetadata("s", new MetadataValue.OfText("x")),
                new MessageMetadata("n", new MetadataValue.OfNumber(-4)),
                new MessageMetadata("b", new MetadataValue.OfBoolean(true)),
                new MessageMetadata("d", new MetadataValue.OfDouble(1.5)),
                new MessageMetadata("o", new MetadataValue.OfObject(payload("T", "1", new byte[0])))));
        assertThat(text).containsOnly(Map.entry("s", "x"), Map.entry("n", "-4"), Map.entry("b", "true"),
                Map.entry("d", "1.5"));
        assertThat(MetadataConverter.toWireMetadata(Map.of("a", "1")))
                .containsExactly(new MessageMetadata("a", new MetadataValue.OfText("1")));
    }
}
