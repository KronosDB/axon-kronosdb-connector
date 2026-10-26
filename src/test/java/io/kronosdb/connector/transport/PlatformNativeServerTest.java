package io.kronosdb.connector.transport;

import io.kronosdb.protocol.ClientIdentity;
import io.kronosdb.protocol.ErrorCode;
import io.kronosdb.protocol.Frame;
import io.kronosdb.protocol.NodeInfo;
import io.kronosdb.protocol.OpenPlatform;
import io.kronosdb.protocol.PlatformInfo;
import io.kronosdb.protocol.ProcessorStatus;
import io.kronosdb.protocol.SegmentStatus;
import io.kronosdb.protocol.ServerHeartbeat;
import io.kronosdb.protocol.TopologyChange;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;

import static io.kronosdb.connector.transport.RealServer.await;
import static io.kronosdb.connector.transport.RealServer.eventually;
import static io.kronosdb.connector.transport.RealServer.ok;
import static io.kronosdb.connector.transport.RealServer.unique;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The platform through {@link PlatformChannel} against a real KronosDB
 * server: registration, the platform stream and its heartbeats, processor
 * reports and the instructions the server sends back. Needs
 * {@code -Dkronosdb.native.admin} to see the server's view of it; see
 * {@link RealServer}.
 */
class PlatformNativeServerTest {

    private final List<Transport> transports = new CopyOnWriteArrayList<>();
    private final List<PlatformChannel> channels = new CopyOnWriteArrayList<>();

    @BeforeEach
    void requireServer() {
        RealServer.require();
    }

    @AfterEach
    void close() {
        channels.forEach(PlatformChannel::disconnect);
        transports.forEach(Transport::close);
    }

    private Transport transport(String clientId) {
        Transport transport = RealServer.transport(clientId);
        transports.add(transport);
        return transport;
    }

    private PlatformChannel platform(Transport transport) {
        PlatformChannel channel = new PlatformChannel(transport, "default");
        channels.add(channel);
        return channel;
    }

    private static ClientIdentity identity(String clientId) {
        return new ClientIdentity(clientId, "orders", "1.2.3", List.of());
    }

    /** The admin console's clients, without whitespace, for substring checks. */
    private static String clients() throws Exception {
        return RealServer.adminGet("/api/v1/clients").replaceAll("\\s", "");
    }

    private static String processors() throws Exception {
        return RealServer.adminGet("/api/v1/processors").replaceAll("\\s", "");
    }

    /** A platform stream opened without a PlatformChannel, so a test decides what the client answers. */
    private static ClientStream openRaw(Transport transport, ClientIdentity identity, List<Frame> frames,
                                        CompletableFuture<Throwable> ended) {
        return transport.connection().open(new OpenPlatform(identity), false, new StreamListener() {
            @Override
            public void onFrame(Frame frame, boolean last) {
                frames.add(frame);
                if (last) {
                    ended.complete(null);
                }
            }

            @Override
            public void onFailure(Throwable cause) {
                ended.complete(cause);
            }
        });
    }

    // --------------------------------------------------------- registration

    @Test
    void registeringTellsTheClientWhichNodeAndContextsItReached() throws Exception {
        PlatformChannel channel = platform(transport(unique("client")));

        PlatformInfo info = await(channel.connect(identity(unique("svc"))));

        assertThat(info.nodeName()).isNotEmpty();
        assertThat(info.version()).isNotEmpty();
        assertThat(info.contexts()).contains("default");
    }

    @Test
    void aClientWithoutAnIdCannotRegister() {
        PlatformChannel channel = platform(transport(unique("client")));

        assertThatThrownBy(() -> await(channel.connect(identity(""))))
                .isInstanceOf(ExecutionException.class)
                .cause()
                .isInstanceOfSatisfying(RequestFailedException.class,
                        e -> assertThat(e.code()).isEqualTo(ErrorCode.INVALID_ARGUMENT));
    }

    // ------------------------------------------------------ stream and heartbeats

    @Test
    void theServerOpensThePlatformStreamWithNodeInfoAndKeepsSendingHeartbeats() throws Exception {
        RealServer.requireShortTimeouts();
        List<Frame> frames = new CopyOnWriteArrayList<>();
        Transport transport = transport(unique("client"));
        ClientStream stream = openRaw(transport, identity(unique("svc")), frames, new CompletableFuture<>());

        eventually("node info and two heartbeats", 10, () -> frames.stream().anyMatch(NodeInfo.class::isInstance)
                && frames.stream().filter(ServerHeartbeat.class::isInstance).count() >= 2);

        NodeInfo node = (NodeInfo) frames.stream().filter(NodeInfo.class::isInstance).findFirst().orElseThrow();
        assertThat(node.nodeName()).isNotEmpty();
        assertThat(node.contexts()).contains("default");
        stream.cancel();
    }

    @Test
    void aClientThatStaysSilentIsCutOffAtTheHeartbeatTimeout() throws Exception {
        RealServer.requireShortTimeouts();
        List<Frame> frames = new CopyOnWriteArrayList<>();
        CompletableFuture<Throwable> ended = new CompletableFuture<>();
        Transport transport = transport(unique("client"));
        openRaw(transport, identity(unique("svc")), frames, ended);

        Throwable cause = await(ended); // the heartbeat timeout is 4 seconds

        assertThat(cause).isInstanceOfSatisfying(RequestFailedException.class, e -> {
            assertThat(e.code()).isEqualTo(ErrorCode.UNAVAILABLE);
            assertThat(e.getMessage()).contains("heartbeat");
        });
    }

    @Test
    void aChannelAnswersTheServersHeartbeatsSoItOutlivesTheHeartbeatTimeout() throws Exception {
        RealServer.requireShortTimeouts();
        RealServer.requireAdmin();
        String clientId = unique("svc");
        PlatformChannel channel = platform(transport(unique("client")));
        channel.openStream(identity(clientId), () -> { });

        Thread.sleep(7000); // longer than the 4-second timeout

        assertThat(clients()).contains("\"client_id\":\"" + clientId + "\"");
    }

    @Test
    void aClientIsListedWhileItsPlatformStreamIsOpenAndGoneWhenItEnds() throws Exception {
        RealServer.requireAdmin();
        String clientId = unique("svc");
        PlatformChannel channel = platform(transport(unique("client")));
        await(channel.connect(identity(clientId)));
        channel.openStream(identity(clientId), () -> { });

        eventually("the client is listed", 10, () -> clients().contains("\"client_id\":\"" + clientId + "\""));
        assertThat(clients()).contains("\"component\":\"orders\"");

        channel.disconnect();

        eventually("the client is gone", 10, () -> !clients().contains("\"client_id\":\"" + clientId + "\""));
    }

    @Test
    void aPlatformStreamWhoseConnectionIsLostIsOpenedAgain() throws Exception {
        RealServer.requireAdmin();
        String clientId = unique("svc");
        Transport transport = transport(unique("client"));
        PlatformChannel channel = platform(transport);
        channel.openStream(identity(clientId), () -> { });
        eventually("the client is listed", 10, () -> clients().contains("\"client_id\":\"" + clientId + "\""));

        transport.connection().close();

        eventually("the client is gone", 10, () -> !clients().contains("\"client_id\":\"" + clientId + "\""));
        eventually("the client is back on a new connection", 20,
                () -> clients().contains("\"client_id\":\"" + clientId + "\""));
    }

    @Test
    void handlerRegistrationsAreAnnouncedOnThePlatformStream() throws Exception {
        List<Frame> frames = new CopyOnWriteArrayList<>();
        Transport watcher = transport(unique("watcher"));
        openRaw(watcher, identity(unique("svc")), frames, new CompletableFuture<>());
        eventually("the stream is open", 10, () -> frames.stream().anyMatch(NodeInfo.class::isInstance));
        String name = unique("Announced");

        Transport handler = transport(unique("handler"));
        await(new CommandChannel(handler, "default", null).registerCommandHandler(
                command -> CompletableFuture.completedFuture(ok(command.messageId(), "ok")),
                10, "announcer", "svc", name).ackFuture());

        eventually("the topology change", 10, () -> frames.stream().anyMatch(f -> f instanceof TopologyChange change
                && change.messageType().equals(name)));
        TopologyChange change = (TopologyChange) frames.stream()
                .filter(f -> f instanceof TopologyChange c && c.messageType().equals(name)).findFirst().orElseThrow();
        assertThat(change.changeType()).isEqualTo("handler_registered");
        assertThat(change.handlerKind()).isEqualTo("command");
        assertThat(change.clientId()).isEqualTo("announcer");
    }

    // ---------------------------------------------------------- processors

    @Test
    void aProcessorReportShowsUpInTheConsole() throws Exception {
        RealServer.requireAdmin();
        String clientId = unique("proc-client");
        String processor = unique("OrderProjection");
        PlatformChannel channel = platform(transport(unique("client")));
        channel.openStream(identity(clientId), () -> { });
        eventually("the client is listed", 10, () -> clients().contains(clientId));

        channel.sendEventProcessorInfo(new ProcessorStatus(processor, "Tracking", 1, 3, true, false, true,
                "store-1", List.of(new SegmentStatus(0, true, false, 1, 42, ""))));

        eventually("the processor is listed", 10, () -> processors().contains("\"name\":\"" + processor + "\""));
        assertThat(processors()).contains("\"token_position\":42");
    }

    @Test
    void anInstructionFromTheConsoleReachesTheClientAndItsOutcomeGoesBack() throws Exception {
        RealServer.requireAdmin();
        String clientId = unique("proc-client");
        String processor = unique("Paused");
        PlatformChannel channel = platform(transport(unique("client")));
        List<PlatformChannel.ProcessorInstruction> received = new CopyOnWriteArrayList<>();
        channel.setInstructionHandler(instruction -> {
            received.add(instruction);
            return CompletableFuture.completedFuture(true);
        });
        channel.openStream(identity(clientId), () -> { });
        eventually("the client is listed", 10, () -> clients().contains(clientId));
        channel.sendEventProcessorInfo(new ProcessorStatus(processor, "Tracking", 1, 3, true, false, true,
                "store-1", List.of(new SegmentStatus(0, true, false, 1, 42, ""))));
        eventually("the processor is listed", 10, () -> processors().contains("\"name\":\"" + processor + "\""));

        String pause = RealServer.adminPost("/api/processors/" + processor + "/pause");
        assertThat(pause.replaceAll("\\s", "")).contains("\"ok\":true");
        eventually("the pause arrives", 10, () -> !received.isEmpty());
        assertThat(received.get(0).type()).isEqualTo(PlatformChannel.ProcessorInstruction.Type.PAUSE);
        assertThat(received.get(0).processorName()).isEqualTo(processor);

        RealServer.adminPost("/api/processors/" + processor + "/split");
        eventually("the split arrives", 10, () -> received.size() >= 2);
        assertThat(received.get(1).type()).isEqualTo(PlatformChannel.ProcessorInstruction.Type.SPLIT);
        assertThat(received.get(1).segmentId()).isZero();

        // The channel acknowledged each; the stream is still healthy.
        assertThat(clients()).contains(clientId);
    }

    @Test
    void anInstructionForAProcessorNobodyRunsHasNoOneToTell() throws Exception {
        RealServer.requireAdmin();

        String result = RealServer.adminPost("/api/processors/" + unique("Nobody") + "/pause");

        assertThat(result.replaceAll("\\s", "")).contains("\"ok\":false");
    }

    @Test
    void aProcessorInstructionHandlerThatFailsDoesNotBreakThePlatformStream() throws Exception {
        RealServer.requireAdmin();
        String clientId = unique("proc-client");
        String processor = unique("Failing");
        PlatformChannel channel = platform(transport(unique("client")));
        CompletableFuture<Void> asked = new CompletableFuture<>();
        channel.setInstructionHandler(instruction -> {
            asked.complete(null);
            return CompletableFuture.failedFuture(new IllegalStateException("cannot pause"));
        });
        channel.openStream(identity(clientId), () -> { });
        eventually("the client is listed", 10, () -> clients().contains(clientId));
        channel.sendEventProcessorInfo(new ProcessorStatus(processor, "Tracking", 1, 3, true, false, true,
                "store-1", List.of(new SegmentStatus(0, true, false, 1, 1, ""))));
        eventually("the processor is listed", 10, () -> processors().contains("\"name\":\"" + processor + "\""));

        RealServer.adminPost("/api/processors/" + processor + "/pause");

        await(asked);
        Thread.sleep(300);
        assertThat(clients()).contains(clientId);
    }
}
