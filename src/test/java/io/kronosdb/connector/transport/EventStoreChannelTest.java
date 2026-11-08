package io.kronosdb.connector.transport;

import io.kronosdb.protocol.Cancel;
import io.kronosdb.protocol.Criterion;
import io.kronosdb.protocol.Events;
import io.kronosdb.protocol.EventRecord;
import io.kronosdb.protocol.GetHead;
import io.kronosdb.protocol.Head;
import io.kronosdb.protocol.Hello;
import io.kronosdb.protocol.Source;
import io.kronosdb.protocol.StoredRecord;
import io.kronosdb.protocol.Subscribe;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The event store channel against a scripted server: what a subscription and
 * a source put on the wire in protocol version 2, where the stream's window
 * and not the request paces the server.
 */
class EventStoreChannelTest {

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

    private EventStoreChannel channel(Script script) throws IOException {
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
        return new EventStoreChannel(transport, "default");
    }

    private static <T> T await(CompletableFuture<T> f) throws Exception {
        return f.get(10, TimeUnit.SECONDS);
    }

    private static StoredRecord record(long position) {
        return new StoredRecord(0, new EventRecord(position, "id-" + position, "Name", "1", 1000 + position,
                List.of(), List.of(), ByteBuffer.wrap(new byte[]{1})));
    }

    @Test
    void aSubscriptionOpensWithEndAndNoCreditsAndDeliversWhatTheServerPushes() throws Exception {
        CompletableFuture<FakeServer.Session.Incoming> opened = new CompletableFuture<>();
        CompletableFuture<FakeServer.Session.Incoming> afterCancel = new CompletableFuture<>();
        EventStoreChannel store = channel(session -> {
            FakeServer.Session.Incoming subscribe = session.next(Subscribe.class);
            opened.complete(subscribe);
            session.send(subscribe.stream(), 0, new Events(List.of(record(7), record(8)), null));
            afterCancel.complete(session.next());
            session.next();
        });
        Criterion criterion = new Criterion(List.of("Name"), List.of());

        try (ResultStream<Events> live = store.stream(7, List.of(criterion))) {
            FakeServer.Session.Incoming subscribe = await(opened);
            assertThat(subscribe.end()).as("the client says nothing more in a subscription").isTrue();
            Subscribe request = (Subscribe) subscribe.frame();
            assertThat(request.from()).isEqualTo(7);
            assertThat(request.criteria()).containsExactly(criterion);
            assertThat(request.batchSize()).as("the server chooses the batch size").isZero();

            Events batch = live.nextIfAvailable(5, TimeUnit.SECONDS);
            assertThat(batch).isNotNull();
            assertThat(batch.events()).extracting(r -> r.event().position()).containsExactly(7L, 8L);
        }
        // Closing the stream is a Cancel, which also carries END.
        FakeServer.Session.Incoming cancel = await(afterCancel);
        assertThat(cancel.frame()).isInstanceOf(Cancel.class);
        assertThat(cancel.stream()).isEqualTo(await(opened).stream());
        assertThat(cancel.end()).isTrue();
    }

    @Test
    void aSubscriptionPassesTheBatchSizeItWasGiven() throws Exception {
        CompletableFuture<Subscribe> opened = new CompletableFuture<>();
        EventStoreChannel store = channel(session -> {
            opened.complete((Subscribe) session.next(Subscribe.class).frame());
            session.next();
        });
        try (ResultStream<Events> ignored = store.stream(0, List.of(), 50)) {
            assertThat(await(opened).batchSize()).isEqualTo(50);
        }
    }

    @Test
    void aSourceOpensWithEnd() throws Exception {
        CompletableFuture<FakeServer.Session.Incoming> opened = new CompletableFuture<>();
        EventStoreChannel store = channel(session -> {
            FakeServer.Session.Incoming source = session.next(Source.class);
            opened.complete(source);
            session.reply(source.stream(), new Events(List.of(record(0)), 1L));
        });
        try (ResultStream<Events> source = store.source(0, List.of())) {
            assertThat(await(opened).end()).isTrue();
            assertThat(source.nextIfAvailable(5, TimeUnit.SECONDS)).isNotNull();
        }
    }

    @Test
    void theHelloCarriesTheConfiguredWindow() throws Exception {
        CompletableFuture<Hello> hello = new CompletableFuture<>();
        FakeServer server = new FakeServer(session -> {
            try {
                hello.complete(session.handshake());
                FakeServer.Session.Incoming in = session.next(GetHead.class);
                session.reply(in.stream(), new Head(5));
            } catch (IOException ignored) {
                // The client hung up.
            }
        });
        closeables.add(server);
        ConnectOptions options = new ConnectOptions("127.0.0.1", server.port(), "t", "c", null, null, 2000, 0, 0, 0,
                256 * 1024);
        try (Transport transport = new Transport(options)) {
            assertThat(await(new EventStoreChannel(transport, "default").head())).isEqualTo(5);
        }
        assertThat(await(hello).window()).isEqualTo(256 * 1024);
    }

    @Test
    void aWindowBelowTheServersMinimumIsRefusedBeforeAnythingIsSent() {
        assertThatThrownBy(() -> new ConnectOptions("127.0.0.1", 1, "t", "c", null, null, 2000, 0, 0, 0, 1000))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("65536");
        assertThatThrownBy(() -> KronosDbConnectionFactory.forClient("t", "c").streamWindow(1000))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("65536");
    }
}
