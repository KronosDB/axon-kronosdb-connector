package io.kronosdb.connector.transport;

import io.kronosdb.protocol.AppendSnapshot;
import io.kronosdb.protocol.BindContext;
import io.kronosdb.protocol.Cancel;
import io.kronosdb.protocol.ClientPong;
import io.kronosdb.protocol.ContextBound;
import io.kronosdb.protocol.CredentialKind;
import io.kronosdb.protocol.Credit;
import io.kronosdb.protocol.ErrorCode;
import io.kronosdb.protocol.EventRecord;
import io.kronosdb.protocol.Events;
import io.kronosdb.protocol.Frame;
import io.kronosdb.protocol.GetHead;
import io.kronosdb.protocol.GetTail;
import io.kronosdb.protocol.GoAway;
import io.kronosdb.protocol.Head;
import io.kronosdb.protocol.Ping;
import io.kronosdb.protocol.Pong;
import io.kronosdb.protocol.RequestFailed;
import io.kronosdb.protocol.ServerPing;
import io.kronosdb.protocol.Source;
import io.kronosdb.protocol.StoredRecord;
import io.kronosdb.protocol.Subscribe;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The client transport against a scripted server: the connection model of the
 * protocol (multiplexed streams, END, RequestFailed, Cancel, pings, GoAway,
 * context binding) without a real KronosDB.
 */
class NativeConnectionTest {

    private final List<AutoCloseable> closeables = new CopyOnWriteArrayList<>();

    @AfterEach
    void close() throws Exception {
        for (AutoCloseable c : closeables) {
            c.close();
        }
    }

    private FakeServer server(java.util.function.Consumer<FakeServer.Session> script) throws IOException {
        FakeServer server = new FakeServer(script);
        closeables.add(server);
        return server;
    }

    static ConnectOptions options(FakeServer server) {
        return new ConnectOptions("127.0.0.1", server.port(), "tester", "client-1", null, null, 2000, 0, 0, 0);
    }

    private NativeConnection connect(FakeServer server) {
        NativeConnection connection = NativeConnection.open(options(server));
        closeables.add(connection);
        return connection;
    }

    @FunctionalInterface
    interface Script {
        void run(FakeServer.Session session) throws Exception;
    }

    private static java.util.function.Consumer<FakeServer.Session> script(Script script) {
        return session -> {
            try {
                session.handshake();
                script.run(session);
            } catch (Exception e) {
                if (!(e instanceof IOException)) {
                    throw new RuntimeException(e);
                }
            }
        };
    }

    private static <T> T await(CompletableFuture<T> future) throws Exception {
        return future.get(5, TimeUnit.SECONDS);
    }

    @Test
    void theHandshakeSendsTheClientsIdentityAndToken() throws Exception {
        AtomicReference<io.kronosdb.protocol.Hello> seen = new AtomicReference<>();
        FakeServer server = new FakeServer(session -> {
            try {
                seen.set(session.handshake());
                session.next();
            } catch (IOException ignored) {
                // Client closed.
            }
        });
        closeables.add(server);
        ConnectOptions options = new ConnectOptions("127.0.0.1", server.port(), "orders-service", "instance-7",
                "s3cret", null, 2000, 0, 0, 0);
        try (NativeConnection connection = NativeConnection.open(options)) {
            assertThat(connection.welcome().serverName()).isEqualTo("fake");
        }
        io.kronosdb.protocol.Hello hello = seen.get();
        assertThat(hello.version()).isEqualTo(1);
        assertThat(hello.recordFormat()).isEqualTo(3);
        assertThat(hello.clientName()).isEqualTo("orders-service");
        assertThat(hello.clientId()).isEqualTo("instance-7");
        assertThat(hello.credentialKind()).isEqualTo(CredentialKind.TOKEN);
        assertThat(hello.credential()).isEqualTo("s3cret");
    }

    @Test
    void withoutATokenTheCredentialKindIsNone() throws Exception {
        AtomicReference<io.kronosdb.protocol.Hello> seen = new AtomicReference<>();
        FakeServer server = new FakeServer(session -> {
            try {
                seen.set(session.handshake());
                session.next();
            } catch (IOException ignored) {
                // Client closed.
            }
        });
        closeables.add(server);
        connect(server).close();
        Thread.sleep(50);
        assertThat(seen.get().credentialKind()).isEqualTo(CredentialKind.NONE);
    }

    @Test
    void aRefusedHandshakeSurfacesTheServersReason() throws Exception {
        FakeServer server = server(session -> {
            try {
                session.readHello();
                session.send(0, 0, new RequestFailed(ErrorCode.UNAUTHENTICATED, "the token is not valid"));
            } catch (IOException ignored) {
                // Client closed.
            }
        });
        assertThatThrownBy(() -> NativeConnection.open(options(server)))
                .isInstanceOf(RequestFailedException.class)
                .hasMessage("the token is not valid")
                .satisfies(e -> assertThat(((RequestFailedException) e).code()).isEqualTo(ErrorCode.UNAUTHENTICATED));
    }

    @Test
    void aServerThatIsNotThereIsAConnectionError() {
        ConnectOptions options = new ConnectOptions("127.0.0.1", 1, "t", "c", null, null, 500, 0, 0, 0);
        assertThatThrownBy(() -> NativeConnection.open(options)).isInstanceOf(ConnectionClosedException.class);
    }

    @Test
    void aUnaryRequestGetsItsAnswer() throws Exception {
        FakeServer server = server(script(session -> {
            FakeServer.Session.Incoming in = session.next(GetHead.class);
            assertThat(in.end()).as("a unary request carries END").isTrue();
            session.reply(in.stream(), new Head(42));
        }));
        Frame reply = await(connect(server).request(new GetHead(0)));
        assertThat(reply).isEqualTo(new Head(42));
    }

    @Test
    void answersMayArriveInAnyOrder() throws Exception {
        FakeServer server = server(script(session -> {
            List<FakeServer.Session.Incoming> requests = new ArrayList<>();
            for (int i = 0; i < 3; i++) {
                requests.add(session.next(GetHead.class));
            }
            for (int i = requests.size() - 1; i >= 0; i--) {
                FakeServer.Session.Incoming r = requests.get(i);
                session.reply(r.stream(), new Head(((GetHead) r.frame()).context()));
            }
        }));
        NativeConnection connection = connect(server);
        CompletableFuture<Frame> a = connection.request(new GetHead(10));
        CompletableFuture<Frame> b = connection.request(new GetHead(20));
        CompletableFuture<Frame> c = connection.request(new GetHead(30));
        assertThat(await(a)).isEqualTo(new Head(10));
        assertThat(await(b)).isEqualTo(new Head(20));
        assertThat(await(c)).isEqualTo(new Head(30));
    }

    @Test
    void aFailedRequestFailsOnlyItself() throws Exception {
        FakeServer server = server(script(session -> {
            while (true) {
                FakeServer.Session.Incoming in = session.next(GetHead.class);
                int context = ((GetHead) in.frame()).context();
                if (context == 1) {
                    session.reply(in.stream(), new RequestFailed(ErrorCode.NOT_FOUND, "no such context"));
                } else {
                    session.reply(in.stream(), new Head(context));
                }
            }
        }));
        NativeConnection connection = connect(server);
        CompletableFuture<Frame> bad = connection.request(new GetHead(1));
        assertThatThrownBy(() -> await(bad))
                .isInstanceOf(ExecutionException.class)
                .cause()
                .isInstanceOfSatisfying(RequestFailedException.class, e -> {
                    assertThat(e.code()).isEqualTo(ErrorCode.NOT_FOUND);
                    assertThat(e.getMessage()).isEqualTo("no such context");
                });
        assertThat(await(connection.request(new GetHead(2)))).isEqualTo(new Head(2));
        assertThat(connection.isClosed()).isFalse();
    }

    @Test
    void aServerStreamDeliversFramesUntilOneCarriesEnd() throws Exception {
        FakeServer server = server(script(session -> {
            FakeServer.Session.Incoming in = session.next(Source.class);
            session.send(in.stream(), 0, new Events(List.of(record(0), record(1)), null));
            session.reply(in.stream(), new Events(List.of(record(2)), 3L));
        }));
        NativeConnection connection = connect(server);
        List<Frame> received = new CopyOnWriteArrayList<>();
        CountDownLatch done = new CountDownLatch(1);
        connection.open(new Source(0, 0, List.of(), 0), true, new StreamListener() {
            @Override
            public void onFrame(Frame frame, boolean last) {
                received.add(frame);
                if (last) {
                    done.countDown();
                }
            }

            @Override
            public void onFailure(Throwable cause) {
                throw new AssertionError(cause);
            }
        });
        assertThat(done.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(received).hasSize(2);
        assertThat(((Events) received.get(1)).marker()).isEqualTo(3L);
    }

    @Test
    void cancellingTellsTheServerAndDropsWhatArrivesLate() throws Exception {
        CompletableFuture<FakeServer.Session.Incoming> cancel = new CompletableFuture<>();
        FakeServer server = server(script(session -> {
            FakeServer.Session.Incoming subscribe = session.next(Subscribe.class);
            assertThat(subscribe.end()).as("a subscription is not ended by the client").isFalse();
            cancel.complete(session.next(Cancel.class));
            // The stream is gone at the client; this must be ignored, not break the connection.
            session.send(subscribe.stream(), 0, new Events(List.of(record(9)), null));
            session.reply(subscribe.stream(), new RequestFailed(ErrorCode.CANCELLED, "cancelled"));
            FakeServer.Session.Incoming next = session.next(GetHead.class);
            session.reply(next.stream(), new Head(1));
        }));
        NativeConnection connection = connect(server);
        List<Frame> received = new CopyOnWriteArrayList<>();
        ClientStream stream = connection.open(
                new Subscribe(0, 0, List.of(), List.of(), 10, 0, 0), false, new StreamListener() {
                    @Override
                    public void onFrame(Frame frame, boolean last) {
                        received.add(frame);
                    }

                    @Override
                    public void onFailure(Throwable cause) {
                        received.add(new Pong(-1));
                    }
                });
        stream.cancel();
        assertThat(await(cancel).end()).as("Cancel carries END").isTrue();
        assertThat(await(connection.request(new GetHead(0)))).isEqualTo(new Head(1));
        assertThat(received).isEmpty();
        assertThat(stream.isOpen()).isFalse();
    }

    @Test
    void creditsFlowOnTheOpenStream() throws Exception {
        CompletableFuture<Credit> credit = new CompletableFuture<>();
        FakeServer server = server(script(session -> {
            session.next(Subscribe.class);
            credit.complete((Credit) session.next(Credit.class).frame());
        }));
        NativeConnection connection = connect(server);
        ClientStream stream = connection.open(new Subscribe(0, 0, List.of(), List.of(), 10, 0, 0), false,
                new StreamListener() {
                    @Override
                    public void onFrame(Frame frame, boolean last) {
                    }

                    @Override
                    public void onFailure(Throwable cause) {
                    }
                });
        stream.send(new Credit(250));
        assertThat(await(credit).credits()).isEqualTo(250);
    }

    @Test
    void theServersPingsAreAnswered() throws Exception {
        CompletableFuture<ClientPong> pong = new CompletableFuture<>();
        FakeServer server = server(script(session -> {
            session.send(0, 0, new ServerPing(77));
            pong.complete((ClientPong) session.next(ClientPong.class).frame());
        }));
        connect(server);
        assertThat(await(pong).nonce()).isEqualTo(77);
    }

    @Test
    void aSilentConnectionIsPingedAndStaysOpenWhileTheServerAnswers() throws Exception {
        AtomicInteger pings = new AtomicInteger();
        FakeServer server = server(script(session -> {
            while (true) {
                FakeServer.Session.Incoming in = session.next(Ping.class);
                pings.incrementAndGet();
                session.send(0, 0, new Pong(((Ping) in.frame()).nonce()));
            }
        }));
        ConnectOptions options = new ConnectOptions("127.0.0.1", server.port(), "t", "c", null, null, 2000, 100, 500, 0);
        NativeConnection connection = NativeConnection.open(options);
        closeables.add(connection);
        Thread.sleep(700);
        assertThat(pings.get()).isGreaterThanOrEqualTo(2);
        assertThat(connection.isClosed()).isFalse();
    }

    @Test
    void aConnectionThatStopsAnsweringIsClosedAndItsRequestsFail() throws Exception {
        FakeServer server = server(script(session -> {
            while (true) {
                session.next(); // Reads everything, answers nothing, not even pings.
            }
        }));
        ConnectOptions options = new ConnectOptions("127.0.0.1", server.port(), "t", "c", null, null, 2000, 100, 200, 0);
        NativeConnection connection = NativeConnection.open(options);
        closeables.add(connection);
        CompletableFuture<Frame> stuck = connection.request(new GetHead(0));
        assertThatThrownBy(() -> stuck.get(5, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class)
                .cause()
                .isInstanceOf(ConnectionClosedException.class);
        assertThat(connection.isClosed()).isTrue();
    }

    @Test
    void whenTheServerHangsUpRequestsInFlightFailAndNewOnesAreRefused() throws Exception {
        CountDownLatch received = new CountDownLatch(1);
        FakeServer server = server(script(session -> {
            session.next(GetHead.class);
            received.countDown();
            session.close();
        }));
        NativeConnection connection = connect(server);
        CompletableFuture<Frame> inFlight = connection.request(new GetHead(0));
        assertThat(received.await(5, TimeUnit.SECONDS)).isTrue();
        assertThatThrownBy(() -> inFlight.get(5, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class)
                .cause()
                .isInstanceOf(ConnectionClosedException.class);
        assertThatThrownBy(() -> await(connection.request(new GetTail(0))))
                .isInstanceOf(ExecutionException.class)
                .cause()
                .isInstanceOf(ConnectionClosedException.class);
    }

    @Test
    void theTransportOpensANewConnectionAfterOneIsLost() throws Exception {
        AtomicInteger served = new AtomicInteger();
        FakeServer server = server(script(session -> {
            FakeServer.Session.Incoming in = session.next(GetHead.class);
            if (served.getAndIncrement() == 0) {
                session.close(); // The first connection dies mid-request.
                return;
            }
            session.reply(in.stream(), new Head(5));
            session.next(); // Hold the connection open.
        }));
        try (Transport transport = new Transport(options(server))) {
            CompletableFuture<Frame> first = transport.connection().request(new GetHead(0));
            assertThatThrownBy(() -> first.get(5, TimeUnit.SECONDS)).isInstanceOf(ExecutionException.class);
            Thread.sleep(300); // Past the pause after a failed attempt.
            assertThat(await(transport.connection().request(new GetHead(0)))).isEqualTo(new Head(5));
        }
        assertThat(server.connections()).isEqualTo(2);
    }

    @Test
    void aGoAwayMakesTheTransportUseANewConnectionWhileTheOldOneFinishes() throws Exception {
        CountDownLatch goneAway = new CountDownLatch(1);
        AtomicInteger accepted = new AtomicInteger();
        FakeServer server = server(script(session -> {
            int me = accepted.getAndIncrement();
            if (me == 0) {
                session.send(0, 0, new GoAway(ErrorCode.UNAVAILABLE, "restarting"));
                goneAway.countDown();
            }
            while (true) {
                FakeServer.Session.Incoming in = session.next(GetHead.class);
                session.reply(in.stream(), new Head(me));
            }
        }));
        try (Transport transport = new Transport(options(server))) {
            NativeConnection first = transport.connection();
            assertThat(goneAway.await(5, TimeUnit.SECONDS)).isTrue();
            Thread.sleep(100);
            NativeConnection second = transport.connection();
            assertThat(second).isNotSameAs(first);
            assertThat(await(second.request(new GetHead(0)))).isEqualTo(new Head(1));
            // The old connection still answers what is on it.
            assertThat(await(first.request(new GetHead(0)))).isEqualTo(new Head(0));
        }
    }

    @Test
    void contextsAreBoundOncePerConnectionAndTheDefaultNeedsNoBinding() throws Exception {
        List<Frame> seen = new CopyOnWriteArrayList<>();
        FakeServer server = server(script(session -> {
            while (true) {
                FakeServer.Session.Incoming in = session.next();
                seen.add(in.frame());
                switch (in.frame()) {
                    case BindContext bind -> session.reply(in.stream(), new ContextBound(bind.id()));
                    case GetHead head -> session.reply(in.stream(), new Head(head.context()));
                    default -> throw new IOException("unexpected " + in.frame());
                }
            }
        }));
        try (Transport transport = new Transport(options(server))) {
            EventStoreChannel orders = new EventStoreChannel(transport, "orders");
            EventStoreChannel defaults = new EventStoreChannel(transport, "default");
            assertThat(await(orders.head())).isEqualTo(1);
            assertThat(await(orders.head())).isEqualTo(1);
            assertThat(await(defaults.head())).isEqualTo(0);
            List<CompletableFuture<Long>> concurrent = new ArrayList<>();
            EventStoreChannel billing = new EventStoreChannel(transport, "billing");
            for (int i = 0; i < 5; i++) {
                concurrent.add(billing.head());
            }
            for (CompletableFuture<Long> f : concurrent) {
                assertThat(await(f)).isEqualTo(2);
            }
        }
        assertThat(seen.stream().filter(f -> f instanceof BindContext).map(f -> ((BindContext) f).name()))
                .containsExactly("orders", "billing");
    }

    @Test
    void aFrameLargerThanTheServersLimitIsRefusedAndTheConnectionStaysUsable() throws Exception {
        FakeServer server = new FakeServer(session -> {
            try {
                session.handshake(1000, 10_000);
                while (true) {
                    FakeServer.Session.Incoming in = session.next();
                    session.reply(in.stream(), new Head(1));
                }
            } catch (IOException ignored) {
                // Client closed.
            }
        });
        closeables.add(server);
        NativeConnection connection = connect(server);
        AppendSnapshot huge = new AppendSnapshot(0, ByteBuffer.wrap(new byte[1]), ByteBuffer.wrap(new byte[5000]), 0);
        assertThatThrownBy(() -> await(connection.request(huge)))
                .isInstanceOf(ExecutionException.class)
                .cause()
                .isInstanceOf(TransportException.class)
                .hasMessageContaining("exceeds the maximum frame size of 1000");
        assertThat(await(connection.request(new GetHead(0)))).isEqualTo(new Head(1));
    }

    @Test
    void aFrameFromTheServerLargerThanTheLimitClosesTheConnection() throws Exception {
        FakeServer server = new FakeServer(session -> {
            try {
                session.handshake(1000, 10_000);
                FakeServer.Session.Incoming in = session.next();
                session.reply(in.stream(), new Events(List.of(new StoredRecord(0, new EventRecord(
                        1, "id", "n", "1", 0, List.of(), List.of(), ByteBuffer.wrap(new byte[4000])))), null));
            } catch (IOException ignored) {
                // Client closed.
            }
        });
        closeables.add(server);
        NativeConnection connection = connect(server);
        assertThatThrownBy(() -> await(connection.request(new GetHead(0))))
                .isInstanceOf(ExecutionException.class)
                .cause()
                .isInstanceOf(ConnectionClosedException.class)
                .hasMessageContaining("exceeds the limit");
    }

    @Test
    void aFrameWithAClientKindFromTheServerIsAProtocolViolation() throws Exception {
        FakeServer server = server(script(session -> {
            FakeServer.Session.Incoming in = session.next(GetHead.class);
            session.send(in.stream(), 0, new GetTail(0)); // A client frame.
        }));
        NativeConnection connection = connect(server);
        assertThatThrownBy(() -> await(connection.request(new GetHead(0))))
                .isInstanceOf(ExecutionException.class)
                .cause()
                .isInstanceOf(ConnectionClosedException.class)
                .hasMessageContaining("client frame");
    }

    @Test
    void manyThreadsSharingOneConnectionEachGetTheirOwnAnswers() throws Exception {
        FakeServer server = server(script(session -> {
            while (true) {
                FakeServer.Session.Incoming in = session.next(GetHead.class);
                session.reply(in.stream(), new Head(((GetHead) in.frame()).context()));
            }
        }));
        NativeConnection connection = connect(server);
        int threads = 32;
        int perThread = 300;
        CountDownLatch go = new CountDownLatch(1);
        List<Thread> workers = new ArrayList<>();
        AtomicInteger wrong = new AtomicInteger();
        AtomicInteger done = new AtomicInteger();
        for (int t = 0; t < threads; t++) {
            int id = t;
            Thread worker = new Thread(() -> {
                try {
                    go.await();
                    for (int i = 0; i < perThread; i++) {
                        int context = id * 1000 + i;
                        Frame reply = connection.request(new GetHead(context % 65536)).get(10, TimeUnit.SECONDS);
                        if (!reply.equals(new Head(context % 65536))) {
                            wrong.incrementAndGet();
                        }
                        done.incrementAndGet();
                    }
                } catch (Exception e) {
                    wrong.incrementAndGet();
                }
            });
            worker.start();
            workers.add(worker);
        }
        go.countDown();
        for (Thread worker : workers) {
            worker.join(60_000);
        }
        assertThat(wrong.get()).isZero();
        assertThat(done.get()).isEqualTo(threads * perThread);
    }

    @Test
    void closingTheConnectionFailsWhatIsInFlight() throws Exception {
        CountDownLatch received = new CountDownLatch(1);
        FakeServer server = server(script(session -> {
            session.next(GetHead.class);
            received.countDown();
            session.next();
        }));
        NativeConnection connection = connect(server);
        CompletableFuture<Frame> pending = connection.request(new GetHead(0));
        assertThat(received.await(5, TimeUnit.SECONDS)).isTrue();
        connection.close();
        assertThatThrownBy(() -> pending.get(5, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class)
                .cause()
                .isInstanceOf(ConnectionClosedException.class);
        assertThat(connection.isClosed()).isTrue();
    }

    private static StoredRecord record(long position) {
        return new StoredRecord(0, new EventRecord(position, "id-" + position, "Name", "1", 1000 + position,
                List.of(), List.of(), ByteBuffer.wrap(new byte[]{1, 2, 3})));
    }
}
