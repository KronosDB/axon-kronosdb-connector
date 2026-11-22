package io.kronosdb.connector.transport;

import io.kronosdb.protocol.Append;
import io.kronosdb.protocol.AppendConflict;
import io.kronosdb.protocol.Appended;
import io.kronosdb.protocol.Condition;
import io.kronosdb.protocol.Criterion;
import io.kronosdb.protocol.ErrorCode;
import io.kronosdb.protocol.EventRecord;
import io.kronosdb.protocol.Frame;
import io.kronosdb.protocol.GetHead;
import io.kronosdb.protocol.Head;
import io.kronosdb.protocol.MetadataEntry;
import io.kronosdb.protocol.RequestFailed;
import io.kronosdb.protocol.StoredRecord;
import io.kronosdb.protocol.Tag;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Appends against a scripted server that answers {@code unavailable} or hangs
 * up before answering: what the channel sends again, and when it stops.
 */
class AppendRetryTest {

    private static final Appended APPENDED = new Appended(7, 2, 9);

    private final List<AutoCloseable> closeables = new CopyOnWriteArrayList<>();

    @AfterEach
    void close() throws Exception {
        for (AutoCloseable c : closeables) {
            c.close();
        }
    }

    @FunctionalInterface
    interface Script {
        /** Runs on one connection, after the handshake; {@code connection} counts them from 0. */
        void run(FakeServer.Session session, int connection) throws Exception;
    }

    private static Consumer<FakeServer.Session> handshakeThen(Script script) {
        AtomicInteger accepted = new AtomicInteger();
        return session -> {
            try {
                session.handshake();
                script.run(session, accepted.getAndIncrement());
            } catch (IOException ignored) {
                // The client hung up.
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
    }

    private FakeServer server(Script script) throws IOException {
        FakeServer server = new FakeServer(handshakeThen(script));
        closeables.add(server);
        return server;
    }

    private Transport transport(int port) {
        Transport transport = new Transport(new ConnectOptions("127.0.0.1", port, "tester", "client-1", null, null,
                2000, 0, 0, 0, ConnectOptions.DEFAULT_WINDOW));
        closeables.add(transport);
        return transport;
    }

    private EventStoreChannel channel(FakeServer server) {
        return new EventStoreChannel(transport(server.port()), "default");
    }

    /** A server that answers every append with {@code unavailable}, numbering its answers from 1. */
    private FakeServer alwaysUnavailable(AtomicInteger appends) throws IOException {
        return server((session, connection) -> {
            while (true) {
                FakeServer.Session.Incoming append = session.next(Append.class);
                session.reply(append.stream(),
                        new RequestFailed(ErrorCode.UNAVAILABLE, "refused " + appends.incrementAndGet()));
            }
        });
    }

    private static <T> T await(CompletableFuture<T> f) throws Exception {
        return f.get(10, TimeUnit.SECONDS);
    }

    private static ByteBuffer utf8(String s) {
        return ByteBuffer.wrap(s.getBytes(StandardCharsets.UTF_8));
    }

    private static StoredRecord event(String identifier) {
        return new StoredRecord(0, new EventRecord(0, identifier, "OrderPlaced", "1", 1_761_300_000_123L,
                List.of(new MetadataEntry("user", "åsa")),
                List.of(new Tag(utf8("order"), utf8("42"))),
                utf8("{\"sku\":\"A-1\"}")));
    }

    private static List<StoredRecord> events() {
        return List.of(event("id-1"), event("id-2"));
    }

    private static List<String> identifiers(FakeServer.Session.Incoming append) {
        return ((Append) append.frame()).events().stream().map(r -> r.event().identifier()).toList();
    }

    /** Answers every GetHead and records every other frame, until the client hangs up. */
    private static void recordTheRest(FakeServer.Session session, List<Frame> seen) throws IOException {
        while (true) {
            FakeServer.Session.Incoming in = session.next();
            if (in.frame() instanceof GetHead) {
                session.reply(in.stream(), new Head(5));
            } else {
                seen.add(in.frame());
            }
        }
    }

    @Test
    void anAppendAnsweredUnavailableIsSentAgainWithTheSameBytes() throws Exception {
        List<FakeServer.Session.Incoming> appends = new CopyOnWriteArrayList<>();
        FakeServer server = server((session, connection) -> {
            FakeServer.Session.Incoming first = session.next(Append.class);
            appends.add(first);
            session.reply(first.stream(), new RequestFailed(ErrorCode.UNAVAILABLE, "not confirmed in time"));
            FakeServer.Session.Incoming second = session.next(Append.class);
            appends.add(second);
            session.reply(second.stream(), APPENDED);
            session.next();
        });

        Appended appended = await(channel(server).append(null, events()));

        assertThat(appended).isEqualTo(APPENDED);
        assertThat(appends).hasSize(2);
        assertThat(appends.get(1).body()).isEqualTo(appends.get(0).body());
        assertThat(identifiers(appends.get(1))).containsExactly("id-1", "id-2");
        assertThat(server.connections()).as("the connection lives on").isEqualTo(1);
    }

    @Test
    void anAppendWhoseConnectionIsLostBeforeTheAnswerIsSentAgainOnANewConnection() throws Exception {
        List<FakeServer.Session.Incoming> appends = new CopyOnWriteArrayList<>();
        FakeServer server = server((session, connection) -> {
            FakeServer.Session.Incoming append = session.next(Append.class);
            appends.add(append);
            if (connection == 0) {
                return; // Hangs up without answering.
            }
            session.reply(append.stream(), APPENDED);
            session.next();
        });

        Appended appended = await(channel(server).append(null, events()));

        assertThat(appended).isEqualTo(APPENDED);
        assertThat(appends).hasSize(2);
        assertThat(appends.get(1).body()).isEqualTo(appends.get(0).body());
        assertThat(identifiers(appends.get(1))).containsExactly("id-1", "id-2");
        assertThat(server.connections()).isEqualTo(2);
    }

    @Test
    void aCommittedTransactionIsSentAgainWithItsConditionAndTheSameEvents() throws Exception {
        List<FakeServer.Session.Incoming> appends = new CopyOnWriteArrayList<>();
        FakeServer server = server((session, connection) -> {
            FakeServer.Session.Incoming append = session.next(Append.class);
            appends.add(append);
            if (connection == 0) {
                return;
            }
            session.reply(append.stream(), APPENDED);
            session.next();
        });
        Condition condition = new Condition(3, List.of(new Criterion(List.of("OrderPlaced"), List.of())));
        EventStoreChannel.AppendTransaction transaction = channel(server).startAppendTransaction();
        transaction.append(condition, List.of(event("id-1")));
        transaction.append(null, List.of(event("id-2")));

        Appended appended = await(transaction.commit());

        assertThat(appended).isEqualTo(APPENDED);
        assertThat(appends).hasSize(2);
        assertThat(appends.get(1).body()).isEqualTo(appends.get(0).body());
        assertThat(((Append) appends.get(1).frame()).condition()).isEqualTo(condition);
        assertThat(identifiers(appends.get(1))).containsExactly("id-1", "id-2");
    }

    @Test
    void aConnectionRefusedAsUnavailableIsOpenedAgainAndTheAppendGoesOut() throws Exception {
        AtomicInteger accepted = new AtomicInteger();
        List<FakeServer.Session.Incoming> appends = new CopyOnWriteArrayList<>();
        FakeServer server = new FakeServer(session -> {
            try {
                if (accepted.getAndIncrement() == 0) {
                    session.readHello();
                    session.send(0, 0, new RequestFailed(ErrorCode.UNAVAILABLE, "too many connections"));
                    return;
                }
                session.handshake();
                FakeServer.Session.Incoming append = session.next(Append.class);
                appends.add(append);
                session.reply(append.stream(), APPENDED);
                session.next();
            } catch (IOException ignored) {
                // The client hung up.
            }
        });
        closeables.add(server);

        Appended appended = await(channel(server).append(null, events()));

        assertThat(appended).isEqualTo(APPENDED);
        assertThat(appends).hasSize(1);
        assertThat(server.connections()).isEqualTo(2);
    }

    @Test
    void anAppendToAServerThatIsNotThereYetGoesOutOnceItIs() throws Exception {
        int port;
        try (ServerSocket probe = new ServerSocket(0, 50, InetAddress.getLoopbackAddress())) {
            port = probe.getLocalPort();
        }
        CompletableFuture<Appended> appended = new EventStoreChannel(transport(port), "default")
                .append(null, events());
        Thread.sleep(300);
        assertThat(appended).isNotDone();

        List<FakeServer.Session.Incoming> appends = new CopyOnWriteArrayList<>();
        closeables.add(new FakeServer(new ServerSocket(port, 50, InetAddress.getLoopbackAddress()),
                handshakeThen((session, connection) -> {
                    FakeServer.Session.Incoming append = session.next(Append.class);
                    appends.add(append);
                    session.reply(append.stream(), APPENDED);
                    session.next();
                })));

        assertThat(await(appended)).isEqualTo(APPENDED);
        assertThat(identifiers(appends.get(0))).containsExactly("id-1", "id-2");
    }

    @Test
    void aConflictIsAnAnswerAndIsNotSentAgain() throws Exception {
        List<Frame> afterwards = new CopyOnWriteArrayList<>();
        FakeServer server = server((session, connection) -> {
            FakeServer.Session.Incoming append = session.next(Append.class);
            session.reply(append.stream(), new AppendConflict(41));
            recordTheRest(session, afterwards);
        });
        EventStoreChannel store = channel(server);
        Condition condition = new Condition(3, List.of(new Criterion(List.of("OrderPlaced"), List.of())));

        assertThatThrownBy(() -> await(store.append(condition, events())))
                .isInstanceOf(ExecutionException.class)
                .cause()
                .isInstanceOfSatisfying(ConsistencyConflictException.class,
                        e -> assertThat(e.conflictingPosition()).isEqualTo(41));

        Thread.sleep(300);
        assertThat(await(store.head())).isEqualTo(5);
        assertThat(afterwards).isEmpty();
    }

    @Test
    void anErrorOtherThanUnavailableIsNotSentAgain() throws Exception {
        List<Frame> afterwards = new CopyOnWriteArrayList<>();
        FakeServer server = server((session, connection) -> {
            FakeServer.Session.Incoming append = session.next(Append.class);
            session.reply(append.stream(), new RequestFailed(ErrorCode.REJECTED, "the context is read-only"));
            recordTheRest(session, afterwards);
        });
        EventStoreChannel store = channel(server);

        assertThatThrownBy(() -> await(store.append(null, events())))
                .isInstanceOf(ExecutionException.class)
                .cause()
                .isInstanceOfSatisfying(RequestFailedException.class, e -> {
                    assertThat(e.code()).isEqualTo(ErrorCode.REJECTED);
                    assertThat(e).hasMessage("the context is read-only");
                });

        Thread.sleep(300);
        assertThat(await(store.head())).isEqualTo(5);
        assertThat(afterwards).isEmpty();
    }

    @Test
    void anAppendStopsBeingSentAgainAfterTheRetryTimeAndFailsWithTheLastError() throws Exception {
        AtomicInteger appends = new AtomicInteger();
        FakeServer server = alwaysUnavailable(appends);
        EventStoreChannel store = new EventStoreChannel(transport(server.port()), "default", 400);
        long started = System.nanoTime();

        assertThatThrownBy(() -> await(store.append(null, events())))
                .isInstanceOf(ExecutionException.class)
                .cause()
                .isInstanceOfSatisfying(RequestFailedException.class, e -> {
                    assertThat(e.code()).isEqualTo(ErrorCode.UNAVAILABLE);
                    assertThat(e).hasMessage("refused " + appends.get());
                });

        long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
        assertThat(elapsed).isBetween(400L, 5000L);
        int sent = appends.get();
        assertThat(sent).isGreaterThan(1);
        Thread.sleep(300);
        assertThat(appends.get()).as("nothing more is sent once it gave up").isEqualTo(sent);
    }

    @Test
    void theWaitBetweenAttemptsGrows() throws Exception {
        List<Long> arrivals = new CopyOnWriteArrayList<>();
        FakeServer server = server((session, connection) -> {
            while (true) {
                FakeServer.Session.Incoming append = session.next(Append.class);
                arrivals.add(System.nanoTime());
                session.reply(append.stream(), arrivals.size() < 4
                        ? new RequestFailed(ErrorCode.UNAVAILABLE, "not yet")
                        : APPENDED);
            }
        });

        assertThat(await(channel(server).append(null, events()))).isEqualTo(APPENDED);

        assertThat(arrivals).hasSize(4);
        long first = TimeUnit.NANOSECONDS.toMillis(arrivals.get(1) - arrivals.get(0));
        long third = TimeUnit.NANOSECONDS.toMillis(arrivals.get(3) - arrivals.get(2));
        assertThat(first).isGreaterThanOrEqualTo(50);
        assertThat(third).isGreaterThanOrEqualTo(200);
    }

    @Test
    void withARetryTimeOfZeroAnUnavailableAppendFailsAtOnce() throws Exception {
        AtomicInteger appends = new AtomicInteger();
        FakeServer server = alwaysUnavailable(appends);
        EventStoreChannel store = new EventStoreChannel(transport(server.port()), "default", 0);

        assertThatThrownBy(() -> await(store.append(null, events())))
                .isInstanceOf(ExecutionException.class)
                .cause()
                .isInstanceOfSatisfying(RequestFailedException.class,
                        e -> assertThat(e.code()).isEqualTo(ErrorCode.UNAVAILABLE));

        Thread.sleep(300);
        assertThat(appends.get()).isEqualTo(1);
    }

    @Test
    void shuttingDownFailsAnAppendThatIsWaitingToBeSentAgain() throws Exception {
        AtomicInteger appends = new AtomicInteger();
        FakeServer server = alwaysUnavailable(appends);
        Transport transport = transport(server.port());
        CompletableFuture<Appended> appended = new EventStoreChannel(transport, "default").append(null, events());
        while (appends.get() < 3) {
            Thread.sleep(10);
        }

        transport.close();

        assertThatThrownBy(() -> appended.get(2, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class)
                .cause()
                .isInstanceOf(TransportException.class);
        int sent = appends.get();
        Thread.sleep(300);
        assertThat(appends.get()).isEqualTo(sent);
    }

    @Test
    void theFactorysRetryTimeReachesItsChannels() throws Exception {
        AtomicInteger appends = new AtomicInteger();
        FakeServer server = alwaysUnavailable(appends);
        KronosDbConnectionFactory factory = KronosDbConnectionFactory.forClient("tester", "client-1")
                .host("127.0.0.1")
                .port(server.port())
                .keepAlive(0, 0)
                .appendRetryTimeout(0)
                .build();
        closeables.add(factory::shutdown);

        assertThatThrownBy(() -> await(factory.connect("default").eventStoreChannel().append(null, events())))
                .isInstanceOf(ExecutionException.class)
                .cause()
                .isInstanceOf(RequestFailedException.class);

        Thread.sleep(300);
        assertThat(appends.get()).isEqualTo(1);
    }

    @Test
    void aNegativeRetryTimeIsRefused() {
        assertThatThrownBy(() -> KronosDbConnectionFactory.forClient("tester", "client-1").appendRetryTimeout(-1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("-1");
    }
}
