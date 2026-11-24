package io.kronosdb.connector.transport;

import io.kronosdb.protocol.Appended;
import io.kronosdb.protocol.Condition;
import io.kronosdb.protocol.Criterion;
import io.kronosdb.protocol.EventRecord;
import io.kronosdb.protocol.Events;
import io.kronosdb.protocol.StoredRecord;
import io.kronosdb.protocol.Tag;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Appends sent again, against a real KronosDB server: the server wrote the
 * append, the client never got the answer, and the same append goes out a
 * second time. Needs a server that recognises it, see
 * {@link RealServer#requireRetrySafeAppends()}.
 */
class AppendRetryNativeServerTest {

    private final List<AutoCloseable> closeables = new CopyOnWriteArrayList<>();

    @BeforeAll
    static void requireServer() {
        RealServer.requireRetrySafeAppends();
    }

    @AfterEach
    void close() throws Exception {
        for (AutoCloseable c : closeables) {
            c.close();
        }
    }

    /** A channel straight to the server, for reading what was written. */
    private EventStoreChannel direct() {
        Transport transport = RealServer.transport(RealServer.unique("reader"));
        closeables.add(transport);
        return new EventStoreChannel(transport, "default");
    }

    /** A channel whose first append loses its answer on the way back. */
    private EventStoreChannel losingTheFirstAnswer(LostAnswerProxy proxy) {
        Transport transport = new Transport(new ConnectOptions("127.0.0.1", proxy.port(), "append-retry-test",
                RealServer.unique("writer"), null, null, 5000, 10_000, 5000, 0, ConnectOptions.DEFAULT_WINDOW));
        closeables.add(transport);
        return new EventStoreChannel(transport, "default");
    }

    private LostAnswerProxy proxy(LostAnswerProxy.Instead instead) throws IOException {
        LostAnswerProxy proxy = new LostAnswerProxy(RealServer.host(), RealServer.port(), instead);
        closeables.add(proxy);
        return proxy;
    }

    private static ByteBuffer utf8(String s) {
        return ByteBuffer.wrap(s.getBytes(StandardCharsets.UTF_8));
    }

    private static Criterion tagged(String value) {
        return new Criterion(List.of(), List.of(new Tag(utf8("it"), utf8(value))));
    }

    private static StoredRecord event(String name, String tag) {
        return new StoredRecord(0, new EventRecord(0, UUID.randomUUID().toString(), name, "1",
                System.currentTimeMillis(), List.of(), List.of(new Tag(utf8("it"), utf8(tag))), utf8(name)));
    }

    private static List<String> identifiers(List<StoredRecord> records) {
        return records.stream().map(r -> r.event().identifier()).toList();
    }

    private static <T> T await(CompletableFuture<T> f) throws Exception {
        return f.get(15, TimeUnit.SECONDS);
    }

    /** Reads everything with the tag; the marker of the read is in {@code marker[0]}. */
    private static List<StoredRecord> read(EventStoreChannel store, String tag, long[] marker) throws Exception {
        List<StoredRecord> all = new ArrayList<>();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
        try (ResultStream<Events> stream = store.source(0, List.of(tagged(tag)))) {
            while (true) {
                Events batch = stream.nextIfAvailable(100, TimeUnit.MILLISECONDS);
                if (batch != null) {
                    all.addAll(batch.events());
                    if (batch.marker() != null) {
                        marker[0] = batch.marker();
                    }
                }
                assertThat(stream.getError()).isEmpty();
                if (stream.isCompleted()) {
                    return all;
                }
                assertThat(System.nanoTime()).as("source did not finish").isLessThan(deadline);
            }
        }
    }

    private void anAppendWhoseAnswerIsLostIsWrittenOnce(LostAnswerProxy.Instead instead) throws Exception {
        String tag = UUID.randomUUID().toString();
        LostAnswerProxy proxy = proxy(instead);
        List<StoredRecord> events = List.of(event("OrderPlaced", tag), event("ItemAdded", tag));

        Appended appended = await(losingTheFirstAnswer(proxy).append(null, events));

        assertThat(proxy.appendedByTheServer()).as("the append reached the server twice").isEqualTo(2);
        List<StoredRecord> written = read(direct(), tag, new long[1]);
        assertThat(identifiers(written)).isEqualTo(identifiers(events));
        assertThat(appended.count()).isEqualTo(2);
        assertThat(appended.firstPosition()).as("the positions the events already had")
                .isEqualTo(written.get(0).event().position());
    }

    @Test
    void anAppendWhoseConnectionIsLostBeforeTheAnswerIsWrittenOnce() throws Exception {
        anAppendWhoseAnswerIsLostIsWrittenOnce(LostAnswerProxy.Instead.A_CLOSED_CONNECTION);
    }

    @Test
    void anAppendAnsweredUnavailableAfterItWasWrittenIsWrittenOnce() throws Exception {
        anAppendWhoseAnswerIsLostIsWrittenOnce(LostAnswerProxy.Instead.UNAVAILABLE);
    }

    @Test
    void aConditionalAppendSentAgainIsNotRefusedByItsOwnEvents() throws Exception {
        String tag = UUID.randomUUID().toString();
        EventStoreChannel direct = direct();
        long[] marker = new long[1];
        assertThat(read(direct, tag, marker)).isEmpty();
        Condition condition = new Condition(marker[0], List.of(tagged(tag)));
        LostAnswerProxy proxy = proxy(LostAnswerProxy.Instead.A_CLOSED_CONNECTION);
        List<StoredRecord> events = List.of(event("AccountOpened", tag));

        Appended appended = await(losingTheFirstAnswer(proxy).append(condition, events));

        assertThat(proxy.appendedByTheServer()).isEqualTo(2);
        List<StoredRecord> written = read(direct, tag, new long[1]);
        assertThat(identifiers(written)).isEqualTo(identifiers(events));
        assertThat(appended.firstPosition()).isEqualTo(written.get(0).event().position());

        // Other events under the same condition are still a conflict.
        assertThatThrownBy(() -> await(direct.append(condition, List.of(event("AccountOpened", tag)))))
                .isInstanceOf(ExecutionException.class)
                .cause()
                .isInstanceOf(ConsistencyConflictException.class);
    }
}
