package io.kronosdb.connector.transport;

import io.kronosdb.protocol.Ack;
import io.kronosdb.protocol.BindBus;
import io.kronosdb.protocol.BusBound;
import io.kronosdb.protocol.Cancel;
import io.kronosdb.protocol.ClientAck;
import io.kronosdb.protocol.ClientIdentity;
import io.kronosdb.protocol.Command;
import io.kronosdb.protocol.CommandHandled;
import io.kronosdb.protocol.CommandResponse;
import io.kronosdb.protocol.CommandResult;
import io.kronosdb.protocol.Credit;
import io.kronosdb.protocol.DispatchCommand;
import io.kronosdb.protocol.DispatchQuery;
import io.kronosdb.protocol.GetPlatformInfo;
import io.kronosdb.protocol.HandleCommand;
import io.kronosdb.protocol.HandleQuery;
import io.kronosdb.protocol.HandleSubscriptionQuery;
import io.kronosdb.protocol.InstructionResult;
import io.kronosdb.protocol.OpenCommandHandlers;
import io.kronosdb.protocol.OpenPlatform;
import io.kronosdb.protocol.OpenQueryHandlers;
import io.kronosdb.protocol.OpenSubscriptionQuery;
import io.kronosdb.protocol.Payload;
import io.kronosdb.protocol.PlatformHeartbeat;
import io.kronosdb.protocol.PlatformInfo;
import io.kronosdb.protocol.ProcessorAction;
import io.kronosdb.protocol.ProcessorInstruction;
import io.kronosdb.protocol.ProcessorStatus;
import io.kronosdb.protocol.Query;
import io.kronosdb.protocol.QueryDone;
import io.kronosdb.protocol.QueryEnd;
import io.kronosdb.protocol.QueryHandled;
import io.kronosdb.protocol.QueryResponse;
import io.kronosdb.protocol.QueryResult;
import io.kronosdb.protocol.QueryUpdate;
import io.kronosdb.protocol.RequestReconnect;
import io.kronosdb.protocol.ServerAck;
import io.kronosdb.protocol.ServerHeartbeat;
import io.kronosdb.protocol.SubscribeCommand;
import io.kronosdb.protocol.SubscribeQuery;
import io.kronosdb.protocol.SubscriptionEnd;
import io.kronosdb.protocol.SubscriptionEvent;
import io.kronosdb.protocol.SubscriptionQuery;
import io.kronosdb.protocol.SubscriptionQueryEvent;
import io.kronosdb.protocol.SubscriptionQueryHandled;
import io.kronosdb.protocol.UnsubscribeCommand;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The command, query and platform channels against a scripted server.
 *
 * <p>The server here is a stand-in written from the schema's documentation of
 * these frames, not the real KronosDB: these tests pin down what the client
 * sends and how it reacts to what the schema says the server sends. They do
 * not show that the real server agrees. That needs a server with native
 * messaging.
 */
class MessagingChannelsTest {

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

    private Transport transport(Script script) throws IOException {
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
        Transport transport = new Transport(NativeConnectionTest.options(server));
        closeables.add(transport);
        return transport;
    }

    private static <T> T await(CompletableFuture<T> f) throws Exception {
        return f.get(10, TimeUnit.SECONDS);
    }

    private static Payload payload(String type, String data) {
        return new Payload(type, "1", ByteBuffer.wrap(data.getBytes()));
    }

    private static Command command(String id, String name) {
        return new Command(id, name, 1000, payload(name, "{}"), List.of(), List.of(), "caller", "orders");
    }

    private static CommandResult ok(String requestId, String data) {
        return new CommandResult("r-" + requestId, requestId, "", null, payload("Result", data), List.of(), List.of());
    }

    private static Query query(String id, String name) {
        return new Query(id, name, 1000, payload(name, "?"), List.of(), List.of(), "caller", "orders");
    }

    private static QueryResult result(String id, String requestId, String data) {
        return new QueryResult(id, requestId, "", null, payload("Answer", data), List.of(), List.of());
    }

    // ------------------------------------------------------------- commands

    @Test
    void aCommandHandlerSubscribesGrantsCreditAndAnswersWhatItIsSent() throws Exception {
        CompletableFuture<OpenCommandHandlers> opened = new CompletableFuture<>();
        CompletableFuture<SubscribeCommand> subscribed = new CompletableFuture<>();
        CompletableFuture<Credit> credit = new CompletableFuture<>();
        CompletableFuture<CommandHandled> handled = new CompletableFuture<>();
        Transport transport = transport(session -> {
            FakeServer.Session.Incoming open = session.next(OpenCommandHandlers.class);
            credit.complete((Credit) session.next(Credit.class).frame());
            assertThat(open.end()).as("a handler stream is not ended by the client").isFalse();
            opened.complete((OpenCommandHandlers) open.frame());
            FakeServer.Session.Incoming subscribe = session.next(SubscribeCommand.class);
            assertThat(subscribe.stream()).isEqualTo(open.stream());
            subscribed.complete((SubscribeCommand) subscribe.frame());
            session.send(open.stream(), 0, new ServerAck(
                    new Ack(((SubscribeCommand) subscribe.frame()).instructionId(), true, null)));
            session.send(open.stream(), 0, new HandleCommand(command("c-1", "PlaceOrder")));
            handled.complete((CommandHandled) session.next(CommandHandled.class).frame());
            session.next();
        });

        CommandChannel channel = new CommandChannel(transport, "default", null);
        List<Command> received = new CopyOnWriteArrayList<>();
        Registration registration = channel.registerCommandHandler(command -> {
            received.add(command);
            return CompletableFuture.completedFuture(ok(command.messageId(), "done"));
        }, 3, "client-1", "orders", "PlaceOrder");

        assertThat(await(opened).bus()).isZero();
        assertThat(await(opened).clientId()).isEqualTo("client-1");
        assertThat(await(opened).componentName()).isEqualTo("orders");
        assertThat(await(subscribed).command()).isEqualTo("PlaceOrder");
        assertThat(await(subscribed).loadFactor()).isEqualTo(3);
        assertThat(await(subscribed).instructionId()).isNotEmpty();
        await(registration.ackFuture());
        assertThat(await(credit).credits()).isEqualTo(500);

        CommandHandled answer = await(handled);
        assertThat(received).extracting(Command::messageId).containsExactly("c-1");
        assertThat(answer.result().requestId()).isEqualTo("c-1");
        assertThat(answer.result().errorCode()).isEmpty();
        assertThat(answer.result().payload().data()).isEqualTo(ByteBuffer.wrap("done".getBytes()));
    }

    @Test
    void aFailingCommandHandlerAnswersWithAnError() throws Exception {
        CompletableFuture<CommandHandled> handled = new CompletableFuture<>();
        Transport transport = transport(session -> {
            FakeServer.Session.Incoming open = session.next(OpenCommandHandlers.class);
            session.next(Credit.class);
            session.next(SubscribeCommand.class);
            session.send(open.stream(), 0, new HandleCommand(command("c-9", "Boom")));
            handled.complete((CommandHandled) session.next(CommandHandled.class).frame());
            session.next();
        });
        CommandChannel channel = new CommandChannel(transport, "default", null);
        channel.registerCommandHandler(
                command -> CompletableFuture.failedFuture(new IllegalStateException("kaput")),
                1, "client-1", "orders", "Boom");

        CommandResult result = await(handled).result();
        assertThat(result.requestId()).isEqualTo("c-9");
        assertThat(result.errorCode()).isEqualTo("COMMAND_EXECUTION_ERROR");
        assertThat(result.error().message()).isEqualTo("kaput");
    }

    @Test
    void unsubscribingTellsTheServer() throws Exception {
        CompletableFuture<UnsubscribeCommand> unsubscribed = new CompletableFuture<>();
        Transport transport = transport(session -> {
            session.next(OpenCommandHandlers.class);
            session.next(Credit.class);
            session.next(SubscribeCommand.class);
            unsubscribed.complete((UnsubscribeCommand) session.next(UnsubscribeCommand.class).frame());
            session.next();
        });
        CommandChannel channel = new CommandChannel(transport, "default", null);
        Registration registration = channel.registerCommandHandler(
                c -> CompletableFuture.completedFuture(ok(c.messageId(), "")), 1, "client-1", "orders", "Foo");
        registration.cancel();
        assertThat(await(unsubscribed).command()).isEqualTo("Foo");
    }

    @Test
    void aHandlerStreamThatDiesIsOpenedAgainAndResubscribed() throws Exception {
        AtomicInteger streams = new AtomicInteger();
        CompletableFuture<SubscribeCommand> again = new CompletableFuture<>();
        Transport transport = transport(session -> {
            session.next(OpenCommandHandlers.class);
            session.next(Credit.class);
            SubscribeCommand subscribe = (SubscribeCommand) session.next(SubscribeCommand.class).frame();
            if (streams.getAndIncrement() == 0) {
                session.close(); // The first connection is lost.
            } else {
                again.complete(subscribe);
                session.next();
            }
        });
        CommandChannel channel = new CommandChannel(transport, "default", null);
        channel.registerCommandHandler(
                c -> CompletableFuture.completedFuture(ok(c.messageId(), "")), 5, "client-1", "orders", "Foo");

        SubscribeCommand resubscribed = again.get(15, TimeUnit.SECONDS);
        assertThat(resubscribed.command()).isEqualTo("Foo");
        assertThat(resubscribed.loadFactor()).isEqualTo(5);
    }

    @Test
    void dispatchingACommandAsksTheServerOnTheBoundBus() throws Exception {
        CompletableFuture<DispatchCommand> dispatched = new CompletableFuture<>();
        List<Object> binds = new CopyOnWriteArrayList<>();
        Transport transport = transport(session -> {
            while (true) {
                FakeServer.Session.Incoming in = session.next();
                switch (in.frame()) {
                    case BindBus bind -> {
                        binds.add(bind.name());
                        session.reply(in.stream(), new BusBound(bind.id()));
                    }
                    case DispatchCommand dispatch -> {
                        dispatched.complete(dispatch);
                        session.reply(in.stream(), new CommandResponse(ok(dispatch.command().messageId(), "yes")));
                    }
                    default -> throw new IOException("unexpected " + in.frame());
                }
            }
        });
        CommandChannel channel = new CommandChannel(transport, "default", "billing");
        CommandResult result = await(channel.sendCommand(command("c-5", "Pay")));

        assertThat(binds).containsExactly("billing");
        assertThat(await(dispatched).bus()).isEqualTo(1);
        assertThat(result.requestId()).isEqualTo("c-5");
        assertThat(result.payload().data()).isEqualTo(ByteBuffer.wrap("yes".getBytes()));
    }

    @Test
    void theDefaultBusNeedsNoBinding() throws Exception {
        CompletableFuture<DispatchCommand> dispatched = new CompletableFuture<>();
        Transport transport = transport(session -> {
            FakeServer.Session.Incoming in = session.next();
            DispatchCommand dispatch = (DispatchCommand) in.frame();
            dispatched.complete(dispatch);
            session.reply(in.stream(), new CommandResponse(ok(dispatch.command().messageId(), "")));
            session.next();
        });
        await(new CommandChannel(transport, "default", null).sendCommand(command("c-1", "X")));
        assertThat(await(dispatched).bus()).isZero();
    }

    @Test
    void disconnectingEndsTheHandlerStream() throws Exception {
        CompletableFuture<FakeServer.Session.Incoming> cancelled = new CompletableFuture<>();
        Transport transport = transport(session -> {
            session.next(OpenCommandHandlers.class);
            session.next(Credit.class);
            session.next(SubscribeCommand.class);
            cancelled.complete(session.next(Cancel.class));
            session.next();
        });
        CommandChannel channel = new CommandChannel(transport, "default", null);
        channel.registerCommandHandler(
                c -> CompletableFuture.completedFuture(ok(c.messageId(), "")), 1, "client-1", "orders", "Foo");
        channel.prepareDisconnect();
        assertThat(await(cancelled).end()).isTrue();
    }

    // -------------------------------------------------------------- queries

    @Test
    void aQueryCollectsEveryHandlersAnswerUntilTheEnd() throws Exception {
        CompletableFuture<DispatchQuery> dispatched = new CompletableFuture<>();
        Transport transport = transport(session -> {
            FakeServer.Session.Incoming in = session.next(DispatchQuery.class);
            assertThat(in.end()).isTrue();
            dispatched.complete((DispatchQuery) in.frame());
            session.send(in.stream(), 0, new QueryResponse(result("a", "q-1", "one")));
            session.send(in.stream(), 0, new QueryResponse(result("b", "q-1", "two")));
            session.reply(in.stream(), new QueryEnd());
            session.next();
        });
        QueryChannel channel = new QueryChannel(transport, "default", null);
        ResultStream<QueryResult> results = channel.query(query("q-1", "FindOrders"));

        List<QueryResult> got = new ArrayList<>();
        while (!results.isCompleted()) {
            QueryResult next = results.nextIfAvailable(100, TimeUnit.MILLISECONDS);
            if (next != null) {
                got.add(next);
            }
        }
        assertThat(await(dispatched).query().name()).isEqualTo("FindOrders");
        assertThat(got).extracting(QueryResult::messageId).containsExactly("a", "b");
        assertThat(results.getError()).isEmpty();
    }

    @Test
    void aQueryThatFailsOnTheServerSurfacesTheError() throws Exception {
        Transport transport = transport(session -> {
            FakeServer.Session.Incoming in = session.next(DispatchQuery.class);
            session.reply(in.stream(), new io.kronosdb.protocol.RequestFailed(
                    io.kronosdb.protocol.ErrorCode.NOT_FOUND, "no handler for FindOrders"));
            session.next();
        });
        ResultStream<QueryResult> results = new QueryChannel(transport, "default", null)
                .query(query("q-1", "FindOrders"));
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!results.isClosed() && System.nanoTime() < deadline) {
            Thread.sleep(10);
        }
        assertThat(results.getError()).get().isInstanceOfSatisfying(RequestFailedException.class,
                e -> assertThat(e.getMessage()).isEqualTo("no handler for FindOrders"));
    }

    @Test
    void aQueryHandlerAnswersWithResultsThenDone() throws Exception {
        CompletableFuture<SubscribeQuery> subscribed = new CompletableFuture<>();
        List<Object> answers = new CopyOnWriteArrayList<>();
        CompletableFuture<Void> done = new CompletableFuture<>();
        Transport transport = transport(session -> {
            FakeServer.Session.Incoming open = session.next(OpenQueryHandlers.class);
            session.next(Credit.class);
            FakeServer.Session.Incoming subscribe = session.next(SubscribeQuery.class);
            subscribed.complete((SubscribeQuery) subscribe.frame());
            session.send(open.stream(), 0, new ServerAck(
                    new Ack(((SubscribeQuery) subscribe.frame()).instructionId(), true, null)));
            session.send(open.stream(), 0, new HandleQuery(query("q-7", "FindOrders")));
            answers.add(session.next(QueryHandled.class).frame());
            answers.add(session.next(QueryHandled.class).frame());
            answers.add(session.next(QueryDone.class).frame());
            done.complete(null);
            session.next();
        });
        QueryChannel channel = new QueryChannel(transport, "default", null);
        Registration registration = channel.registerQueryHandler(new QueryChannel.QueryHandler() {
            @Override
            public void handle(Query query, Consumer<QueryResult> responseSender, Runnable onComplete) {
                responseSender.accept(result("x", query.messageId(), "1"));
                responseSender.accept(result("y", query.messageId(), "2"));
                onComplete.run();
            }

            @Override
            public Registration registerSubscriptionQuery(SubscriptionQuery query,
                                                          QueryChannel.SubscriptionUpdateSender updateSender) {
                throw new AssertionError();
            }
        }, "FindOrders", "OrderList", "client-1", "orders");

        await(registration.ackFuture());
        assertThat(await(subscribed).query()).isEqualTo("FindOrders");
        assertThat(await(subscribed).resultName()).isEqualTo("OrderList");
        await(done);
        assertThat(((QueryHandled) answers.get(0)).result().requestId()).isEqualTo("q-7");
        assertThat(((QueryHandled) answers.get(1)).result().messageId()).isEqualTo("y");
        assertThat(((QueryDone) answers.get(2)).requestId()).isEqualTo("q-7");
    }

    @Test
    void aSubscriptionQueryGetsTheInitialResultUpdatesAndTheEnd() throws Exception {
        CompletableFuture<OpenSubscriptionQuery> opened = new CompletableFuture<>();
        CompletableFuture<Credit> credit = new CompletableFuture<>();
        CompletableFuture<FakeServer.Session.Incoming> cancel = new CompletableFuture<>();
        Transport transport = transport(session -> {
            FakeServer.Session.Incoming open = session.next(OpenSubscriptionQuery.class);
            assertThat(open.end()).isFalse();
            OpenSubscriptionQuery frame = (OpenSubscriptionQuery) open.frame();
            opened.complete(frame);
            String id = frame.subscription().subscriptionId();
            session.send(open.stream(), 0, new SubscriptionQueryEvent(id,
                    new SubscriptionEvent.OfInitialResult(result("init", "q", "first"))));
            session.send(open.stream(), 0, new SubscriptionQueryEvent(id,
                    new SubscriptionEvent.OfUpdate(new QueryUpdate("u1", payload("Update", "next"), List.of(),
                            "h", "orders", "", null))));
            credit.complete((Credit) session.next(Credit.class).frame());
            session.send(open.stream(), 0, new SubscriptionQueryEvent(id,
                    new SubscriptionEvent.OfComplete(new SubscriptionEnd("h", "orders"))));
            cancel.complete(session.next(Cancel.class));
            session.next();
        });
        QueryChannel channel = new QueryChannel(transport, "default", null);
        QueryChannel.SubscriptionQueryResult subscription = channel.subscriptionQuery(query("q", "Watch"), 32);

        assertThat(await(opened).subscription().permits()).isEqualTo(32);
        assertThat(subscription.subscriptionId()).isEqualTo(await(opened).subscription().subscriptionId());
        ResultStream<SubscriptionEvent> results = subscription.results();
        List<SubscriptionEvent> got = new ArrayList<>();
        subscription.sendFlowControl(8);
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (got.size() < 3 && System.nanoTime() < deadline) {
            SubscriptionEvent next = results.nextIfAvailable(100, TimeUnit.MILLISECONDS);
            if (next != null) {
                got.add(next);
            }
        }
        assertThat(got).hasSize(3);
        assertThat(got.get(0)).isInstanceOf(SubscriptionEvent.OfInitialResult.class);
        assertThat(got.get(1)).isInstanceOf(SubscriptionEvent.OfUpdate.class);
        assertThat(got.get(2)).isInstanceOf(SubscriptionEvent.OfComplete.class);
        assertThat(await(credit).credits()).isEqualTo(8);

        subscription.close();
        assertThat(await(cancel).end()).isTrue();
    }

    @Test
    void aSubscriptionQueryHandlerAnswersInitialResultThenUpdatesAndStopsWhenCancelled() throws Exception {
        List<SubscriptionQueryHandled> sent = new CopyOnWriteArrayList<>();
        CompletableFuture<Void> three = new CompletableFuture<>();
        Transport transport = transport(session -> {
            FakeServer.Session.Incoming open = session.next(OpenQueryHandlers.class);
            session.next(Credit.class);
            session.next(SubscribeQuery.class);
            session.send(open.stream(), 0, new HandleSubscriptionQuery(
                    new SubscriptionQuery("sub-1", query("q-1", "Watch"), 16)));
            for (int i = 0; i < 3; i++) {
                sent.add((SubscriptionQueryHandled) session.next(SubscriptionQueryHandled.class).frame());
            }
            three.complete(null);
            session.send(open.stream(), 0, new io.kronosdb.protocol.CancelSubscriptionQuery("sub-1"));
            session.next();
        });
        QueryChannel channel = new QueryChannel(transport, "default", null);
        AtomicReference<QueryChannel.SubscriptionUpdateSender> sender = new AtomicReference<>();
        CompletableFuture<Void> cancelled = new CompletableFuture<>();
        channel.registerQueryHandler(new QueryChannel.QueryHandler() {
            @Override
            public void handle(Query query, Consumer<QueryResult> responseSender, Runnable onComplete) {
                responseSender.accept(result("init", query.messageId(), "now"));
                onComplete.run();
            }

            @Override
            public Registration registerSubscriptionQuery(SubscriptionQuery query,
                                                          QueryChannel.SubscriptionUpdateSender updateSender) {
                sender.set(updateSender);
                // The initial result is sent right after this returns; updates follow it.
                CompletableFuture.runAsync(() -> {
                    try {
                        Thread.sleep(200);
                    } catch (InterruptedException ignored) {
                        return;
                    }
                    updateSender.sendUpdate(new QueryUpdate("u1", payload("Update", "later"), List.of(), "h",
                            "orders", "", null));
                    updateSender.complete("h", "orders");
                });
                return new Registration(() -> cancelled.complete(null));
            }
        }, "Watch", "", "client-1", "orders");

        await(three);
        assertThat(sent).extracting(SubscriptionQueryHandled::subscriptionId).containsOnly("sub-1");
        assertThat(sent.get(0).event()).isInstanceOf(SubscriptionEvent.OfInitialResult.class);
        assertThat(sent.get(1).event()).isInstanceOf(SubscriptionEvent.OfUpdate.class);
        assertThat(sent.get(2).event()).isInstanceOf(SubscriptionEvent.OfComplete.class);
        await(cancelled);
    }

    @Test
    void updatesBeyondTheSubscribersCreditsWaitForMoreAndTheCompletionFollowsThem() throws Exception {
        List<Object> sent = new CopyOnWriteArrayList<>();
        CompletableFuture<Void> initial = new CompletableFuture<>();
        CompletableFuture<Void> all = new CompletableFuture<>();
        Transport transport = transport(session -> {
            FakeServer.Session.Incoming open = session.next(OpenQueryHandlers.class);
            session.next(Credit.class);
            session.next(SubscribeQuery.class);
            // The subscriber's window is two updates.
            session.send(open.stream(), 0, new HandleSubscriptionQuery(
                    new SubscriptionQuery("sub-1", query("q-1", "Watch"), 2)));
            sent.add(session.next(SubscriptionQueryHandled.class).frame());
            initial.complete(null);
            // The handler emits five updates and a completion; two updates and nothing else may pass.
            sent.add(session.next(SubscriptionQueryHandled.class).frame());
            sent.add(session.next(SubscriptionQueryHandled.class).frame());
            Thread.sleep(300);
            sent.add("granted");
            session.send(open.stream(), 0, new io.kronosdb.protocol.SubscriptionQueryPermits("sub-1", 3));
            for (int i = 0; i < 4; i++) {
                sent.add(session.next(SubscriptionQueryHandled.class).frame());
            }
            all.complete(null);
            session.next();
        });
        QueryChannel channel = new QueryChannel(transport, "default", null);
        AtomicReference<QueryChannel.SubscriptionUpdateSender> sender = new AtomicReference<>();
        channel.registerQueryHandler(new QueryChannel.QueryHandler() {
            @Override
            public void handle(Query query, Consumer<QueryResult> responseSender, Runnable onComplete) {
                responseSender.accept(result("init", query.messageId(), "now"));
                onComplete.run();
            }

            @Override
            public Registration registerSubscriptionQuery(SubscriptionQuery query,
                                                          QueryChannel.SubscriptionUpdateSender updateSender) {
                sender.set(updateSender);
                return new Registration(() -> { });
            }
        }, "Watch", "", "client-1", "orders");

        await(initial);
        for (int i = 1; i <= 5; i++) {
            sender.get().sendUpdate(new QueryUpdate("u" + i, payload("Update", "n" + i), List.of(), "h", "orders", "", null));
        }
        sender.get().complete("h", "orders");
        await(all);

        List<String> events = sent.stream().map(frame -> {
            if (frame instanceof SubscriptionQueryHandled handled) {
                return switch (handled.event()) {
                    case SubscriptionEvent.OfInitialResult ignored -> "initial";
                    case SubscriptionEvent.OfUpdate update -> update.value().messageId();
                    case SubscriptionEvent.OfComplete ignored -> "complete";
                    default -> "other";
                };
            }
            return frame.toString();
        }).toList();
        assertThat(events).containsExactly("initial", "u1", "u2", "granted", "u3", "u4", "u5", "complete");
    }

    // ------------------------------------------------------------- platform

    @Test
    void thePlatformStreamHeartbeatsReportsProcessorsAndObeysInstructions() throws Exception {
        ClientIdentity identity = new ClientIdentity("client-1", "orders", "0.1.0", List.of());
        CompletableFuture<GetPlatformInfo> info = new CompletableFuture<>();
        CompletableFuture<OpenPlatform> opened = new CompletableFuture<>();
        CompletableFuture<ProcessorStatus> status = new CompletableFuture<>();
        List<Object> afterInstruction = new CopyOnWriteArrayList<>();
        CompletableFuture<Void> heartbeat = new CompletableFuture<>();
        Transport transport = transport(session -> {
            while (true) {
                FakeServer.Session.Incoming in = session.next();
                switch (in.frame()) {
                    case GetPlatformInfo get -> {
                        info.complete(get);
                        session.reply(in.stream(), new PlatformInfo("node-a", "0.10.1", List.of("default")));
                    }
                    case OpenPlatform open -> {
                        opened.complete(open);
                        session.send(in.stream(), 0, new ServerHeartbeat());
                        session.send(in.stream(), 0, new ProcessorInstruction("i-1", ProcessorAction.SPLIT_SEGMENT,
                                "projector", 3));
                    }
                    case PlatformHeartbeat ignored -> heartbeat.complete(null);
                    case ProcessorStatus s -> status.complete(s);
                    case ClientAck ack -> afterInstruction.add(ack.ack());
                    case InstructionResult result -> {
                        afterInstruction.add(result);
                        session.send(in.stream(), 0, new RequestReconnect());
                    }
                    default -> throw new IOException("unexpected " + in.frame());
                }
            }
        });
        PlatformChannel channel = new PlatformChannel(transport, "default");
        closeables.add(channel::disconnect);

        PlatformInfo platform = await(channel.connect(identity));
        assertThat(platform.nodeName()).isEqualTo("node-a");
        assertThat(await(info).client()).isEqualTo(identity);

        AtomicReference<PlatformChannel.ProcessorInstruction> instruction = new AtomicReference<>();
        channel.setInstructionHandler(i -> {
            instruction.set(i);
            return CompletableFuture.completedFuture(true);
        });
        CompletableFuture<Void> reconnectRequested = new CompletableFuture<>();
        channel.openStream(identity, () -> reconnectRequested.complete(null));

        assertThat(await(opened).client()).isEqualTo(identity);
        await(heartbeat); // The server's heartbeat is answered.
        await(reconnectRequested);
        assertThat(instruction.get()).isEqualTo(new PlatformChannel.ProcessorInstruction(
                PlatformChannel.ProcessorInstruction.Type.SPLIT, "projector", 3));
        assertThat(afterInstruction).hasSize(2);
        assertThat(((Ack) afterInstruction.get(0)).instructionId()).isEqualTo("i-1");
        assertThat(((Ack) afterInstruction.get(0)).success()).isTrue();
        assertThat(afterInstruction.get(1)).isEqualTo(new InstructionResult("i-1", true, null));

        ProcessorStatus report = new ProcessorStatus("projector", "Pooled Streaming", 1, 3, true, false, true,
                "store", List.of());
        channel.sendEventProcessorInfo(report);
        assertThat(await(status)).isEqualTo(report);
    }

    @Test
    void periodicHeartbeatsGoOnThePlatformStream() throws Exception {
        AtomicInteger heartbeats = new AtomicInteger();
        Transport transport = transport(session -> {
            while (true) {
                FakeServer.Session.Incoming in = session.next();
                if (in.frame() instanceof PlatformHeartbeat) {
                    heartbeats.incrementAndGet();
                }
            }
        });
        PlatformChannel channel = new PlatformChannel(transport, "default");
        closeables.add(channel::disconnect);
        channel.openStream(new ClientIdentity("client-1", "orders", "0.1.0", List.of()), () -> { });
        channel.enableHeartbeat(50);
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (heartbeats.get() < 3 && System.nanoTime() < deadline) {
            Thread.sleep(20);
        }
        assertThat(heartbeats.get()).isGreaterThanOrEqualTo(3);
    }
}
