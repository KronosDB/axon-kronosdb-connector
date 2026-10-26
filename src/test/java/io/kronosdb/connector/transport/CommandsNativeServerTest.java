package io.kronosdb.connector.transport;

import io.kronosdb.protocol.Command;
import io.kronosdb.protocol.CommandResult;
import io.kronosdb.protocol.Credit;
import io.kronosdb.protocol.ErrorCode;
import io.kronosdb.protocol.ErrorDetail;
import io.kronosdb.protocol.MessageMetadata;
import io.kronosdb.protocol.MetadataValue;
import io.kronosdb.protocol.OpenCommandHandlers;
import io.kronosdb.protocol.ProcessingInstruction;
import io.kronosdb.protocol.ProcessingKey;
import io.kronosdb.protocol.SubscribeCommand;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static io.kronosdb.connector.transport.RealServer.await;
import static io.kronosdb.connector.transport.RealServer.command;
import static io.kronosdb.connector.transport.RealServer.eventually;
import static io.kronosdb.connector.transport.RealServer.ok;
import static io.kronosdb.connector.transport.RealServer.text;
import static io.kronosdb.connector.transport.RealServer.unique;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Command handlers and dispatch through {@link CommandChannel} against a real
 * KronosDB server. Dispatchers and handlers use transports of their own, as
 * they do in separate application instances. See {@link RealServer} for how
 * to run these.
 */
class CommandsNativeServerTest {

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

    private CommandChannel channel(Transport transport) {
        return new CommandChannel(transport, "default", null);
    }

    /** A handler on its own transport that answers {@code name} with "tag:body". */
    private Transport echoHandler(String name, String tag, int loadFactor) throws Exception {
        Transport transport = transport(unique("handler-" + tag));
        Registration registration = channel(transport).registerCommandHandler(
                command -> CompletableFuture.completedFuture(
                        ok(command.messageId(), tag + ":" + text(command.payload()))),
                loadFactor, unique("client-" + tag), "echo-service", name);
        await(registration.ackFuture());
        return transport;
    }

    /** Dispatches until the handler's registration has reached the routing table. */
    private static CommandResult dispatchWhenReady(CommandChannel dispatcher, Function<Integer, Command> command)
            throws Exception {
        for (int attempt = 0; attempt < 200; attempt++) {
            try {
                return await(dispatcher.sendCommand(command.apply(attempt)));
            } catch (java.util.concurrent.ExecutionException e) {
                if (e.getCause() instanceof RequestFailedException failed
                        && failed.code() == ErrorCode.UNAVAILABLE) {
                    Thread.sleep(50);
                    continue;
                }
                throw e;
            }
        }
        throw new AssertionError("the command never reached a handler");
    }

    private static CommandResult dispatchWhenReady(CommandChannel dispatcher, Command command) throws Exception {
        return dispatchWhenReady(dispatcher, attempt -> new Command(command.messageId() + "-" + attempt,
                command.name(), command.timestamp(), command.payload(), command.metadata(), command.instructions(),
                command.clientId(), command.componentName()));
    }

    private static String owner(CommandResult result) {
        return text(result.payload()).split(":")[0];
    }

    // ------------------------------------------------------------- dispatch

    @Test
    void aCommandGoesToItsHandlerAndTheAnswerComesBackWithItsMetadata() throws Exception {
        String name = unique("Echo");
        Transport handler = transport(unique("handler"));
        List<Command> received = new CopyOnWriteArrayList<>();
        await(channel(handler).registerCommandHandler(command -> {
            received.add(command);
            return CompletableFuture.completedFuture(new CommandResult(
                    "r-1", command.messageId(), "", null, RealServer.payload("Receipt", "hello " + text(command.payload())),
                    List.of(new MessageMetadata("trace", new MetadataValue.OfText("t-9")),
                            new MessageMetadata("attempts", new MetadataValue.OfNumber(3))),
                    List.of()));
        }, 10, "client-echo", "echo-service", name).ackFuture());

        Command sent = new Command("c-" + name, name, System.currentTimeMillis(), RealServer.payload(name, "wörld"),
                List.of(new MessageMetadata("user", new MetadataValue.OfText("åsa")),
                        new MessageMetadata("count", new MetadataValue.OfNumber(7))),
                List.of(new ProcessingInstruction(ProcessingKey.ROUTING_KEY, new MetadataValue.OfText("order-7")),
                        new ProcessingInstruction(ProcessingKey.PRIORITY, new MetadataValue.OfNumber(5))),
                "caller-1", "web");
        CommandResult result = dispatchWhenReady(channel(transport(unique("dispatcher"))), sent);

        assertThat(result.errorCode()).isEmpty();
        assertThat(result.requestId()).isEqualTo(received.get(0).messageId());
        assertThat(result.payload().typeName()).isEqualTo("Receipt");
        assertThat(text(result.payload())).isEqualTo("hello wörld");
        assertThat(result.metadata()).containsExactlyInAnyOrder(
                new MessageMetadata("trace", new MetadataValue.OfText("t-9")),
                new MessageMetadata("attempts", new MetadataValue.OfNumber(3)));

        Command arrived = received.get(0);
        assertThat(arrived.name()).isEqualTo(name);
        assertThat(arrived.clientId()).isEqualTo("caller-1");
        assertThat(arrived.componentName()).isEqualTo("web");
        assertThat(arrived.payload().typeName()).isEqualTo(name);
        assertThat(arrived.metadata()).containsExactlyInAnyOrder(
                new MessageMetadata("user", new MetadataValue.OfText("åsa")),
                new MessageMetadata("count", new MetadataValue.OfNumber(7)));
        assertThat(arrived.instructions()).containsExactlyInAnyOrder(
                new ProcessingInstruction(ProcessingKey.ROUTING_KEY, new MetadataValue.OfText("order-7")),
                new ProcessingInstruction(ProcessingKey.PRIORITY, new MetadataValue.OfNumber(5)));
    }

    @Test
    void aHandlerThatThrowsAnswersWithACommandExecutionError() throws Exception {
        String name = unique("Boom");
        Transport handler = transport(unique("handler"));
        await(channel(handler).registerCommandHandler(
                command -> CompletableFuture.failedFuture(new IllegalStateException("kaput")),
                10, "client-boom", "svc", name).ackFuture());

        CommandResult result = dispatchWhenReady(channel(transport(unique("dispatcher"))), command(name, "x"));

        assertThat(result.errorCode()).isEqualTo("COMMAND_EXECUTION_ERROR");
        assertThat(result.error().message()).isEqualTo("kaput");
        assertThat(result.payload()).isNull();
    }

    @Test
    void aHandlersOwnFailureTravelsWithItsCodeAndDetails() throws Exception {
        String name = unique("Refuse");
        Transport handler = transport(unique("handler"));
        await(channel(handler).registerCommandHandler(
                command -> CompletableFuture.completedFuture(new CommandResult("r", command.messageId(), "APP-1",
                        new ErrorDetail("no stock", "warehouse-3", List.of("sku A-1", "wanted 5"), "APP-1"),
                        null, List.of(), List.of())),
                10, "client-refuse", "svc", name).ackFuture());

        CommandResult result = dispatchWhenReady(channel(transport(unique("dispatcher"))), command(name, "x"));

        assertThat(result.errorCode()).isEqualTo("APP-1");
        assertThat(result.error()).isEqualTo(
                new ErrorDetail("no stock", "warehouse-3", List.of("sku A-1", "wanted 5"), "APP-1"));
    }

    @Test
    void aCommandNobodyHandlesFailsAsUnavailable() throws Exception {
        CommandChannel dispatcher = channel(transport(unique("dispatcher")));

        assertThatThrownBy(() -> await(dispatcher.sendCommand(command(unique("Nobody"), "x"))))
                .isInstanceOf(ExecutionException.class)
                .cause()
                .isInstanceOfSatisfying(RequestFailedException.class,
                        e -> assertThat(e.code()).isEqualTo(ErrorCode.UNAVAILABLE));
    }

    @Test
    void aCommandWithoutANameIsRejectedAsInvalid() throws Exception {
        CommandChannel dispatcher = channel(transport(unique("dispatcher")));

        assertThatThrownBy(() -> await(dispatcher.sendCommand(command("", "x"))))
                .isInstanceOf(ExecutionException.class)
                .cause()
                .isInstanceOfSatisfying(RequestFailedException.class,
                        e -> assertThat(e.code()).isEqualTo(ErrorCode.INVALID_ARGUMENT));
    }

    // -------------------------------------------------- load factor and keys

    @Test
    void unkeyedCommandsAreSharedInProportionToTheLoadFactors() throws Exception {
        String name = unique("Weighted");
        echoHandler(name, "light", 1);
        echoHandler(name, "heavy", 3);
        CommandChannel dispatcher = channel(transport(unique("dispatcher")));
        Set<String> seen = new HashSet<>();
        for (int probe = 0; seen.size() < 2; probe++) {
            assertThat(probe).as("both handlers are in the routing table").isLessThan(500);
            seen.add(owner(dispatchWhenReady(dispatcher, command(name, "probe"))));
        }

        Map<String, Integer> counts = new HashMap<>();
        for (int i = 0; i < 200; i++) {
            counts.merge(owner(await(dispatcher.sendCommand(command(name, "x")))), 1, Integer::sum);
        }

        // Weights 1:3 over 200 commands: 50 and 150, give or take a few that
        // other tests' commands shift in the bus-wide rotation.
        assertThat(counts.get("light")).isBetween(35, 65);
        assertThat(counts.get("heavy")).isBetween(135, 165);
    }

    @Test
    void aRoutingKeyAlwaysReachesTheSameHandler() throws Exception {
        String name = unique("Keyed");
        echoHandler(name, "alpha", 100);
        echoHandler(name, "beta", 100);
        CommandChannel dispatcher = channel(transport(unique("dispatcher")));
        dispatchWhenReady(dispatcher, keyed(name, "warm"));

        // While registrations are still being replicated, the server routes
        // a keyed command by the handlers it has so far, so a key can change
        // hands. Measure once the owners of a dozen keys have stopped moving
        // and both handlers own some.
        Map<String, String> owners = new HashMap<>();
        int stableRounds = 0;
        for (int round = 0; stableRounds < 5; round++) {
            assertThat(round).as("the owners of the keys settle").isLessThan(500);
            Map<String, String> now = new HashMap<>();
            for (int key = 0; key < 12; key++) {
                now.put("order-" + key, owner(await(dispatcher.sendCommand(keyed(name, "order-" + key)))));
            }
            stableRounds = now.equals(owners) && new HashSet<>(now.values()).size() == 2 ? stableRounds + 1 : 0;
            owners = now;
            Thread.sleep(20);
        }

        for (int key = 0; key < 12; key++) {
            for (int n = 0; n < 6; n++) {
                assertThat(owner(await(dispatcher.sendCommand(keyed(name, "order-" + key)))))
                        .as("owner of order-%d", key).isEqualTo(owners.get("order-" + key));
            }
        }
    }

    private static Command keyed(String name, String key) {
        Command base = command(name, "x");
        return new Command(base.messageId(), name, base.timestamp(), base.payload(), List.of(),
                List.of(new ProcessingInstruction(ProcessingKey.ROUTING_KEY, new MetadataValue.OfText(key))),
                base.clientId(), base.componentName());
    }

    // -------------------------------------------------- names, permits, unsubscribe

    @Test
    void oneHandlerStreamServesSeveralCommandNamesRegisteredTogetherOrLater() throws Exception {
        String a = unique("A"), b = unique("B"), c = unique("C");
        Transport handler = transport(unique("handler"));
        CommandChannel handlers = channel(handler);
        Function<Command, CompletableFuture<CommandResult>> echo = command -> CompletableFuture.completedFuture(
                ok(command.messageId(), command.name().split("-")[0] + ":" + text(command.payload())));
        await(handlers.registerCommandHandler(echo, 10, "client-multi", "svc", a, b).ackFuture());
        await(handlers.registerCommandHandler(echo, 10, "client-multi", "svc", c).ackFuture());

        CommandChannel dispatcher = channel(transport(unique("dispatcher")));
        for (String name : List.of(a, b, c)) {
            assertThat(text(dispatchWhenReady(dispatcher, command(name, "ping")).payload()))
                    .isEqualTo(name.split("-")[0] + ":ping");
        }
    }

    @Test
    void anUnsubscribedCommandFindsNoHandlerAnymoreWhileTheOthersKeepWorking() throws Exception {
        String kept = unique("Kept"), dropped = unique("Dropped");
        Transport handler = transport(unique("handler"));
        CommandChannel handlers = channel(handler);
        Function<Command, CompletableFuture<CommandResult>> echo =
                command -> CompletableFuture.completedFuture(ok(command.messageId(), "ok"));
        Registration keptRegistration = handlers.registerCommandHandler(echo, 10, "client-un", "svc", kept);
        Registration droppedRegistration = handlers.registerCommandHandler(echo, 10, "client-un", "svc", dropped);
        await(keptRegistration.ackFuture());
        await(droppedRegistration.ackFuture());
        CommandChannel dispatcher = channel(transport(unique("dispatcher")));
        dispatchWhenReady(dispatcher, command(dropped, "x"));

        droppedRegistration.cancel();

        eventually("the dropped command has no handler", 10, () -> {
            try {
                dispatcher.sendCommand(command(dropped, "x")).get();
                return false;
            } catch (Exception e) {
                return e.getCause() instanceof RequestFailedException failed
                        && failed.code() == ErrorCode.UNAVAILABLE;
            }
        });
        assertThat(await(dispatcher.sendCommand(command(kept, "x"))).errorCode()).isEmpty();
    }

    @Test
    void aHandlerKeepsGrantingPermitsSoMoreCommandsThanTheFirstWindowAllGetThrough() throws Exception {
        String name = unique("Many");
        AtomicInteger handled = new AtomicInteger();
        Transport handler = transport(unique("handler"));
        await(channel(handler).registerCommandHandler(command -> {
            handled.incrementAndGet();
            return CompletableFuture.completedFuture(ok(command.messageId(), "ok"));
        }, 10, "client-many", "svc", name).ackFuture());
        CommandChannel dispatcher = channel(transport(unique("dispatcher")));
        dispatchWhenReady(dispatcher, command(name, "warm"));

        // The first window is 500 permits; three times that in flight at 64
        // at a time only completes if the handler refills it as it goes.
        int total = 1500;
        List<CompletableFuture<CommandResult>> results = new ArrayList<>();
        for (int i = 0; i < total; i++) {
            results.add(dispatcher.sendCommand(command(name, "n" + i)));
            if (results.size() >= 64) {
                await(results.removeFirst());
            }
        }
        for (CompletableFuture<CommandResult> result : results) {
            assertThat(await(result).errorCode()).isEmpty();
        }
        assertThat(handled.get()).isEqualTo(total + 1);
    }

    // ------------------------------------------------- failures and recovery

    @Test
    void aHandlerThatDisconnectsFailsItsCommandsInFlightAndComesBackOnItsOwn() throws Exception {
        String name = unique("Slow");
        CompletableFuture<String> received = new CompletableFuture<>();
        AtomicInteger calls = new AtomicInteger();
        Transport handler = transport(unique("handler"));
        await(channel(handler).registerCommandHandler(command -> {
            if (calls.getAndIncrement() == 0) {
                received.complete(command.messageId());
                return new CompletableFuture<>(); // never answered
            }
            return CompletableFuture.completedFuture(ok(command.messageId(), "back"));
        }, 10, "client-slow", "svc", name).ackFuture());
        CommandChannel dispatcher = channel(transport(unique("dispatcher")));

        CompletableFuture<CommandResult> inFlight = RealServer.async(
                () -> dispatchWhenReady(dispatcher, command(name, "x")));
        await(received);
        handler.connection().close();

        CommandResult lost = await(inFlight);
        assertThat(lost.errorCode()).isEqualTo("KRONOSDB-4006");
        assertThat(lost.requestId()).as("the lost command").isNotEmpty();

        // The channel reopens its handler stream on a new connection and
        // subscribes again; commands flow once more.
        CommandResult again = dispatchWhenReady(dispatcher, command(name, "again"));
        assertThat(again.errorCode()).isEmpty();
        assertThat(text(again.payload())).isEqualTo("back");
    }

    @Test
    void aCommandThatNoHandlerAnswersInTimeIsAnsweredInBandAsTimedOut() throws Exception {
        RealServer.requireShortTimeouts();
        String name = unique("Hang");
        CompletableFuture<Void> received = new CompletableFuture<>();
        Transport handler = transport(unique("handler"));
        await(channel(handler).registerCommandHandler(command -> {
            received.complete(null);
            return new CompletableFuture<>();
        }, 10, "client-hang", "svc", name).ackFuture());
        CommandChannel dispatcher = channel(transport(unique("dispatcher")));

        CommandResult result = dispatchWhenReady(dispatcher, command(name, "x"));

        assertThat(received).isCompleted();
        assertThat(result.errorCode()).isEqualTo("KRONOSDB-4005");
    }

    @Test
    void aNewerHandlerStreamOfTheSameClientTakesOverAndTheOlderOneIsEndedAsUnavailable() throws Exception {
        String name = unique("Twin");
        String clientId = unique("client-twin");
        // A stream a lost connection left behind: the server still has it open.
        Transport stale = transport(unique("stale"));
        CompletableFuture<Throwable> ended = new CompletableFuture<>();
        ClientStream old = stale.connection().open(new OpenCommandHandlers(0, clientId, "svc"), false,
                new StreamListener() {
                    @Override
                    public void onFrame(io.kronosdb.protocol.Frame frame, boolean last) {
                    }

                    @Override
                    public void onFailure(Throwable cause) {
                        ended.complete(cause);
                    }
                });
        old.send(new SubscribeCommand("s-1", name, 100));
        old.send(new Credit(10));

        Transport fresh = transport(unique("fresh"));
        await(channel(fresh).registerCommandHandler(
                command -> CompletableFuture.completedFuture(ok(command.messageId(), "new")),
                10, clientId, "svc", name).ackFuture());

        assertThat(await(ended)).isInstanceOfSatisfying(RequestFailedException.class,
                e -> assertThat(e.code()).isEqualTo(ErrorCode.UNAVAILABLE));
        // Ending the old stream must not take the new stream's registration with it.
        CommandResult result = dispatchWhenReady(channel(transport(unique("dispatcher"))), command(name, "x"));
        assertThat(text(result.payload())).isEqualTo("new");
        Thread.sleep(300);
        assertThat(text(await(channel(transport(unique("dispatcher2"))).sendCommand(command(name, "y"))).payload()))
                .isEqualTo("new");
    }
}
