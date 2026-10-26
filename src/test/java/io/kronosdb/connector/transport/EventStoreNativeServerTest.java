package io.kronosdb.connector.transport;

import io.kronosdb.protocol.Appended;
import io.kronosdb.protocol.Condition;
import io.kronosdb.protocol.Criterion;
import io.kronosdb.protocol.EventRecord;
import io.kronosdb.protocol.Events;
import io.kronosdb.protocol.MetadataEntry;
import io.kronosdb.protocol.Snapshot;
import io.kronosdb.protocol.StoredRecord;
import io.kronosdb.protocol.Tag;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * The event store path against a real KronosDB server. Runs when a server is
 * given:
 *
 * <pre>
 * mvn test -Dkronosdb.native.addr=host.docker.internal:50051
 * </pre>
 *
 * and is skipped otherwise. Tests keep to their own tags, so they can share a
 * server with each other and with anything else on it.
 */
class EventStoreNativeServerTest {

    private static final String ADDRESS = System.getProperty("kronosdb.native.addr");

    private static Transport transport;
    private static EventStoreChannel store;

    @BeforeAll
    static void connect() {
        assumeTrue(ADDRESS != null, "set -Dkronosdb.native.addr=host:port to run against a server");
        int colon = ADDRESS.lastIndexOf(':');
        transport = new Transport(new ConnectOptions(ADDRESS.substring(0, colon),
                Integer.parseInt(ADDRESS.substring(colon + 1)), "event-store-test", UUID.randomUUID().toString(),
                null, null, 5000, 10_000, 5000, 0));
        store = new EventStoreChannel(transport, "default");
    }

    @AfterAll
    static void disconnect() {
        if (transport != null) {
            transport.close();
        }
    }

    private static ByteBuffer utf8(String s) {
        return ByteBuffer.wrap(s.getBytes(StandardCharsets.UTF_8));
    }

    private static String text(ByteBuffer b) {
        return StandardCharsets.UTF_8.decode(b.duplicate()).toString();
    }

    private static Criterion tagged(String value) {
        return new Criterion(List.of(), List.of(new Tag(utf8("it"), utf8(value))));
    }

    private static StoredRecord event(String name, String tag, String payload) {
        return new StoredRecord(0, new EventRecord(0, UUID.randomUUID().toString(), name, "1",
                System.currentTimeMillis(),
                List.of(new MetadataEntry("origin", "tést ✓")),
                List.of(new Tag(utf8("it"), utf8(tag)), new Tag(utf8("kind"), utf8(name))),
                utf8(payload)));
    }

    /** Reads a source stream to its end; the marker is in the last batch. */
    private static List<StoredRecord> drain(ResultStream<Events> stream, long[] marker) throws Exception {
        List<StoredRecord> all = new ArrayList<>();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
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

    private static <T> T await(CompletableFuture<T> f) throws Exception {
        return f.get(10, TimeUnit.SECONDS);
    }

    @Test
    void appendedEventsAreReadBackWithEverythingIntact() throws Exception {
        String tag = UUID.randomUUID().toString();
        Appended appended = await(store.append(null, List.of(
                event("OrderPlaced", tag, "first ünï"),
                event("ItemAdded", tag, "second"))));
        assertThat(appended.count()).isEqualTo(2);
        assertThat(appended.marker()).isGreaterThanOrEqualTo(appended.firstPosition() + 2);

        long[] marker = new long[1];
        List<StoredRecord> read = drain(store.source(0, List.of(tagged(tag))), marker);
        assertThat(read).hasSize(2);
        assertThat(read.get(0).event().position()).isEqualTo(appended.firstPosition());
        assertThat(read.get(0).event().name()).isEqualTo("OrderPlaced");
        assertThat(read.get(0).event().version()).isEqualTo("1");
        assertThat(text(read.get(0).event().payload())).isEqualTo("first ünï");
        assertThat(read.get(0).event().metadata()).containsExactly(new MetadataEntry("origin", "tést ✓"));
        assertThat(read.get(0).event().tags()).extracting(t -> text(t.key()) + "=" + text(t.value()))
                .containsExactlyInAnyOrder("it=" + tag, "kind=OrderPlaced");
        assertThat(text(read.get(1).event().payload())).isEqualTo("second");
        assertThat(read.get(1).event().position()).isGreaterThan(read.get(0).event().position());
        assertThat(marker[0]).isGreaterThan(read.get(1).event().position());
    }

    @Test
    void criteriaSelectByTypeAndTag() throws Exception {
        String tag = UUID.randomUUID().toString();
        await(store.append(null, List.of(event("A", tag, "1"), event("B", tag, "2"), event("A", tag, "3"))));
        Criterion onlyA = new Criterion(List.of("A"), tagged(tag).tags());
        List<StoredRecord> read = drain(store.source(0, List.of(onlyA)), new long[1]);
        assertThat(read).extracting(r -> text(r.event().payload())).containsExactly("1", "3");
    }

    @Test
    void aConditionalAppendFailsWhenSomethingMatchingCameAfterTheMarker() throws Exception {
        String tag = UUID.randomUUID().toString();
        Condition none = new Condition(0, List.of(tagged(tag)));
        Appended first = await(store.append(none, List.of(event("Opened", tag, "a"))));

        // The same condition, from before the first event: it conflicts with it.
        assertThatThrownBy(() -> await(store.append(none, List.of(event("Opened", tag, "b")))))
                .isInstanceOf(ExecutionException.class)
                .cause()
                .isInstanceOfSatisfying(ConsistencyConflictException.class, e ->
                        assertThat(e.conflictingPosition()).isEqualTo(first.firstPosition()));

        // From the marker the first append returned, nothing conflicts.
        Condition afterFirst = new Condition(first.marker(), List.of(tagged(tag)));
        Appended second = await(store.append(afterFirst, List.of(event("Closed", tag, "c"))));
        assertThat(second.firstPosition()).isGreaterThan(first.firstPosition());

        List<StoredRecord> read = drain(store.source(0, List.of(tagged(tag))), new long[1]);
        assertThat(read).extracting(r -> text(r.event().payload())).containsExactly("a", "c");
    }

    @Test
    void aSubscriptionDeliversNewEventsAndRefillsItsCredits() throws Exception {
        String tag = UUID.randomUUID().toString();
        long from = await(store.head());
        // Few credits and many events: the stream only keeps going if credits are granted back.
        ResultStream<Events> live = store.stream(from, List.of(tagged(tag)), 50);
        try {
            int batches = 6;
            int perBatch = 200;
            for (int b = 0; b < batches; b++) {
                List<StoredRecord> events = new ArrayList<>();
                for (int i = 0; i < perBatch; i++) {
                    events.add(event("Tick", tag, String.valueOf(b * perBatch + i)));
                }
                await(store.append(null, events));
            }
            int expected = batches * perBatch;
            int seen = 0;
            long lastPosition = -1;
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            while (seen < expected) {
                Events batch = live.nextIfAvailable(200, TimeUnit.MILLISECONDS);
                if (batch == null) {
                    assertThat(live.getError()).isEmpty();
                    assertThat(System.nanoTime()).as("stalled after %d of %d events", seen, expected)
                            .isLessThan(deadline);
                    continue;
                }
                for (StoredRecord r : batch.events()) {
                    assertThat(text(r.event().payload())).isEqualTo(String.valueOf(seen));
                    assertThat(r.event().position()).isGreaterThan(lastPosition);
                    lastPosition = r.event().position();
                    seen++;
                }
            }
        } finally {
            live.close();
        }
    }

    @Test
    void closingASubscriptionLeavesTheConnectionUsable() throws Exception {
        ResultStream<Events> live = store.stream(await(store.head()), List.of(tagged(UUID.randomUUID().toString())));
        Thread.sleep(100);
        live.close();
        assertThat(live.isClosed()).isTrue();
        assertThat(await(store.head())).isNotNegative();
    }

    @Test
    void cancellingASourceMidwayLeavesTheConnectionUsable() throws Exception {
        String tag = UUID.randomUUID().toString();
        List<StoredRecord> many = new ArrayList<>();
        for (int i = 0; i < 3000; i++) {
            many.add(event("Bulk", tag, "x".repeat(200)));
        }
        await(store.append(null, many));
        ResultStream<Events> source = store.source(0, List.of(tagged(tag)));
        Events first = null;
        for (int i = 0; i < 100 && first == null; i++) {
            first = source.nextIfAvailable(100, TimeUnit.MILLISECONDS);
        }
        assertThat(first).isNotNull();
        source.close();
        assertThat(await(store.head())).isPositive();
    }

    @Test
    void headTailTagsAndPositionAt() throws Exception {
        String tag = UUID.randomUUID().toString();
        long before = System.currentTimeMillis() - 1;
        Appended appended = await(store.append(null, List.of(event("T", tag, "x"))));

        assertThat(await(store.head())).isGreaterThanOrEqualTo(appended.firstPosition() + 1);
        assertThat(await(store.tail())).isLessThanOrEqualTo(appended.firstPosition());
        assertThat(await(store.getTags(appended.firstPosition())))
                .extracting(t -> text(t.key()) + "=" + text(t.value()))
                .containsExactlyInAnyOrder("it=" + tag, "kind=T");
        assertThat(await(store.getSequenceAt(before))).isGreaterThanOrEqualTo(0);
        assertThat(await(store.getSequenceAt(4_102_444_800_000L))).isEqualTo(-1);
    }

    @Test
    void snapshotsAreStoredPerKeyAndTheNewestWins() throws Exception {
        ByteBuffer key = utf8("snapshot-" + UUID.randomUUID());
        assertThat(await(store.getSnapshot(key))).isNull();

        long head = await(store.head());
        await(store.appendSnapshot(key, utf8("state one"), head));
        Snapshot one = await(store.getSnapshot(key));
        assertThat(one).isNotNull();
        assertThat(text(one.state())).isEqualTo("state one");
        assertThat(one.position()).isEqualTo(head);

        await(store.appendSnapshot(key, utf8("state two"), head + 1));
        Snapshot two = await(store.getSnapshot(key));
        assertThat(text(two.state())).isEqualTo("state two");
        assertThat(two.position()).isEqualTo(head + 1);

        assertThat(await(store.getSnapshot(utf8("snapshot-" + UUID.randomUUID())))).isNull();
    }

    @Test
    void manyThreadsAppendOnOneConnection() throws Exception {
        String tag = UUID.randomUUID().toString();
        int threads = 64;
        int perThread = 25;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        try {
            List<Future<Set<Long>>> futures = new ArrayList<>();
            for (int t = 0; t < threads; t++) {
                int id = t;
                futures.add(pool.submit(() -> {
                    Set<Long> positions = new HashSet<>();
                    for (int i = 0; i < perThread; i++) {
                        positions.add(await(store.append(null, List.of(event("Par", tag, id + ":" + i))))
                                .firstPosition());
                    }
                    return positions;
                }));
            }
            Set<Long> all = new HashSet<>();
            for (Future<Set<Long>> f : futures) {
                all.addAll(f.get(60, TimeUnit.SECONDS));
            }
            assertThat(all).hasSize(threads * perThread);
        } finally {
            pool.shutdownNow();
        }
        List<StoredRecord> read = drain(store.source(0, List.of(tagged(tag))), new long[1]);
        assertThat(read).hasSize(threads * perThread);
    }

    @Test
    void anUnknownContextIsRefusedByTheServer() {
        EventStoreChannel nowhere = new EventStoreChannel(transport, "no-such-context-" + UUID.randomUUID());
        assertThatThrownBy(() -> await(nowhere.head()))
                .isInstanceOf(ExecutionException.class)
                .cause()
                .isInstanceOf(RequestFailedException.class);
    }
}
