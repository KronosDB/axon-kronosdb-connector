package io.kronosdb.connector.axon.event;

import io.kronosdb.connector.axon.snapshot.KronosDbSnapshotStore;
import io.kronosdb.connector.transport.KronosDbConnection;
import io.kronosdb.connector.transport.KronosDbConnectionFactory;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.conversion.Converter;
import org.axonframework.eventsourcing.eventstore.AppendCondition;
import org.axonframework.eventsourcing.eventstore.AppendEventsTransactionRejectedException;
import org.axonframework.eventsourcing.eventstore.ConsistencyMarker;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;
import org.axonframework.eventsourcing.eventstore.GenericTaggedEventMessage;
import org.axonframework.eventsourcing.eventstore.GlobalIndexConsistencyMarker;
import org.axonframework.eventsourcing.eventstore.GlobalIndexPosition;
import org.axonframework.eventsourcing.eventstore.SourcingCondition;
import org.axonframework.eventsourcing.eventstore.TaggedEventMessage;
import org.axonframework.eventsourcing.snapshot.api.Snapshot;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.axonframework.eventsourcing.eventstore.TerminalEventMessage;
import org.axonframework.messaging.eventhandling.conversion.DelegatingEventConverter;
import org.axonframework.messaging.eventhandling.conversion.EventConverter;
import org.axonframework.messaging.eventhandling.processing.streaming.token.GlobalSequenceTrackingToken;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.axonframework.messaging.eventstreaming.EventCriteria;
import org.axonframework.messaging.eventstreaming.StreamingCondition;
import org.axonframework.messaging.eventstreaming.Tag;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * The Axon-facing event store path against a real KronosDB server: the same
 * calls Axon makes, through {@link KronosDbEventStorageEngine} and
 * {@link KronosDbSnapshotStore}. Needs {@code -Dkronosdb.native.addr=host:port}.
 */
class KronosDbEventStorageEngineServerTest {

    private static final String ADDRESS = System.getProperty("kronosdb.native.addr");

    private static KronosDbConnectionFactory factory;
    private static KronosDbConnection connection;
    private static EventStorageEngine engine;

    /** Payloads are byte arrays already; the converter hands them back and turns strings into bytes. */
    private static final Converter PASS_THROUGH = new Converter() {
        @SuppressWarnings("unchecked")
        @Override
        public <T> T convert(Object input, Type targetType) {
            if (input instanceof String text && targetType == byte[].class) {
                return (T) text.getBytes(StandardCharsets.UTF_8);
            }
            return (T) input;
        }

        @Override
        public void describeTo(ComponentDescriptor descriptor) {
        }
    };

    @BeforeAll
    static void connect() {
        assumeTrue(ADDRESS != null, "set -Dkronosdb.native.addr=host:port to run against a server");
        int colon = ADDRESS.lastIndexOf(':');
        factory = KronosDbConnectionFactory.forClient("engine-test", UUID.randomUUID().toString())
                .host(ADDRESS.substring(0, colon))
                .port(Integer.parseInt(ADDRESS.substring(colon + 1)))
                .build();
        connection = factory.connect("default");
        EventConverter converter = new DelegatingEventConverter(PASS_THROUGH);
        engine = new KronosDbEventStorageEngine(connection, converter);
    }

    @AfterAll
    static void disconnect() {
        if (factory != null) {
            factory.shutdown();
        }
    }

    private static TaggedEventMessage<?> event(String name, String tag, String payload) {
        EventMessage message = new GenericEventMessage(
                UUID.randomUUID().toString(),
                new MessageType(name, "2"),
                payload.getBytes(StandardCharsets.UTF_8),
                Map.of("user", "åsa"),
                Instant.ofEpochMilli(1_761_300_000_123L));
        return new GenericTaggedEventMessage<>(message, Set.of(Tag.of("it", tag)));
    }

    private static ConsistencyMarker append(AppendCondition condition, TaggedEventMessage<?>... events)
            throws Exception {
        EventStorageEngine.AppendTransaction<?> tx = engine.appendEvents(condition, null, List.of(events))
                .get(10, TimeUnit.SECONDS);
        return commit(tx);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static ConsistencyMarker commit(EventStorageEngine.AppendTransaction tx) throws Exception {
        Object result = tx.commit().get(10, TimeUnit.SECONDS);
        return (ConsistencyMarker) tx.afterCommit(result).get(10, TimeUnit.SECONDS);
    }

    /** Pulls a message stream until it completes. */
    private static List<MessageStream.Entry<EventMessage>> read(MessageStream<EventMessage> stream) throws Exception {
        List<MessageStream.Entry<EventMessage>> entries = new ArrayList<>();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
        while (true) {
            Optional<MessageStream.Entry<EventMessage>> next = stream.next();
            if (next.isPresent()) {
                entries.add(next.get());
                continue;
            }
            assertThat(stream.error()).isEmpty();
            if (stream.isCompleted()) {
                return entries;
            }
            assertThat(System.nanoTime()).as("stream did not complete").isLessThan(deadline);
            Thread.sleep(5);
        }
    }

    @Test
    void sourcingReturnsTheEventsAndTheConsistencyMarker() throws Exception {
        String tag = UUID.randomUUID().toString();
        append(AppendCondition.none(), event("OrderPlaced", tag, "one"), event("ItemAdded", tag, "two"));

        List<MessageStream.Entry<EventMessage>> entries = read(engine.source(
                SourcingCondition.conditionFor(EventCriteria.havingTags(Tag.of("it", tag)))));

        assertThat(entries).hasSize(3);
        EventMessage first = entries.get(0).message();
        assertThat(first.type()).isEqualTo(new MessageType("OrderPlaced", "2"));
        assertThat(first.payloadAs(byte[].class, PASS_THROUGH)).isEqualTo("one".getBytes(StandardCharsets.UTF_8));
        assertThat(first.metadata()).containsEntry("user", "åsa");
        assertThat(first.timestamp()).isEqualTo(Instant.ofEpochMilli(1_761_300_000_123L));
        TrackingToken token = entries.get(0).getResource(TrackingToken.RESOURCE_KEY);
        assertThat(token).isInstanceOf(GlobalSequenceTrackingToken.class);

        assertThat(entries.get(1).message().type().name()).isEqualTo("ItemAdded");
        assertThat(entries.get(2).message()).isSameAs(TerminalEventMessage.INSTANCE);
        ConsistencyMarker marker = entries.get(2).getResource(ConsistencyMarker.RESOURCE_KEY);
        TrackingToken lastToken = entries.get(1).getResource(TrackingToken.RESOURCE_KEY);
        // The marker is just past the last event read, which is what the last token also is.
        assertThat(GlobalIndexConsistencyMarker.position(marker))
                .isGreaterThanOrEqualTo(lastToken.position().getAsLong());
    }

    @Test
    void anAppendConditionFromAStaleMarkerIsRejected() throws Exception {
        String tag = UUID.randomUUID().toString();
        EventCriteria criteria = EventCriteria.havingTags(Tag.of("it", tag));
        append(AppendCondition.none(), event("Opened", tag, "a"));

        // Sourcing yields the marker a decision was made at.
        List<MessageStream.Entry<EventMessage>> sourced = read(engine.source(SourcingCondition.conditionFor(criteria)));
        ConsistencyMarker marker = sourced.get(sourced.size() - 1).getResource(ConsistencyMarker.RESOURCE_KEY);

        // Someone else appends a matching event first.
        append(AppendCondition.none(), event("Closed", tag, "b"));

        EventStorageEngine.AppendTransaction<?> tx = engine.appendEvents(
                AppendCondition.withCriteria(criteria).withMarker(marker), null,
                List.of(event("Reopened", tag, "c"))).get(10, TimeUnit.SECONDS);
        assertThatThrownBy(() -> tx.commit().get(10, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class)
                .cause()
                .isInstanceOf(AppendEventsTransactionRejectedException.class)
                .hasMessageContaining("conflicting event at position");

        // Nothing was written by the rejected append.
        List<MessageStream.Entry<EventMessage>> after = read(engine.source(SourcingCondition.conditionFor(criteria)));
        assertThat(after).hasSize(3);
        assertThat(after.get(0).message().type().name()).isEqualTo("Opened");
        assertThat(after.get(1).message().type().name()).isEqualTo("Closed");
        assertThat(after.get(2).message()).isSameAs(TerminalEventMessage.INSTANCE);
    }

    @Test
    void streamingDeliversEventsAppendedAfterItStartedWithTheirTokens() throws Exception {
        String tag = UUID.randomUUID().toString();
        EventCriteria criteria = EventCriteria.havingTags(Tag.of("it", tag));
        TrackingToken start = engine.latestToken().get(10, TimeUnit.SECONDS);

        MessageStream<EventMessage> stream = engine.stream(StreamingCondition.conditionFor(start, criteria));
        try {
            append(AppendCondition.none(), event("Streamed", tag, "1"), event("Streamed", tag, "2"));
            List<MessageStream.Entry<EventMessage>> got = new ArrayList<>();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (got.size() < 2 && System.nanoTime() < deadline) {
                stream.next().ifPresentOrElse(got::add, () -> {
                    try {
                        Thread.sleep(5);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });
            }
            assertThat(got).hasSize(2);
            assertThat(got.get(0).message().payloadAs(byte[].class, PASS_THROUGH))
                    .isEqualTo("1".getBytes(StandardCharsets.UTF_8));
            TrackingToken first = got.get(0).getResource(TrackingToken.RESOURCE_KEY);
            TrackingToken second = got.get(1).getResource(TrackingToken.RESOURCE_KEY);
            assertThat(second.position().getAsLong()).isGreaterThan(first.position().getAsLong());
        } finally {
            stream.close();
        }
    }

    @Test
    void tokensFollowTheLog() throws Exception {
        String tag = UUID.randomUUID().toString();
        Instant before = Instant.now().minusSeconds(1);
        append(AppendCondition.none(), event("Tokenised", tag, "x"));

        long first = engine.firstToken().get(10, TimeUnit.SECONDS).position().getAsLong();
        long latest = engine.latestToken().get(10, TimeUnit.SECONDS).position().getAsLong();
        long at = engine.tokenAt(before).get(10, TimeUnit.SECONDS).position().getAsLong();
        assertThat(first).isLessThanOrEqualTo(at);
        assertThat(at).isLessThan(latest);
        // Nothing is that recent: the token is the start, as before the port.
        assertThat(engine.tokenAt(Instant.parse("2100-01-01T00:00:00Z")).get(10, TimeUnit.SECONDS).position()
                .getAsLong()).isZero();
    }

    @Test
    void snapshotsRoundTripThroughTheSnapshotStore() throws Exception {
        KronosDbSnapshotStore store = new KronosDbSnapshotStore(connection, PASS_THROUGH);
        QualifiedName name = new QualifiedName("Order");
        String id = UUID.randomUUID().toString();

        assertThat(store.load(name, id, null).get(10, TimeUnit.SECONDS)).isNull();

        Snapshot snapshot = new Snapshot(new GlobalIndexPosition(
                engine.latestToken().get(10, TimeUnit.SECONDS).position().getAsLong()),
                "3", "state".getBytes(StandardCharsets.UTF_8), Instant.ofEpochMilli(1_761_300_000_000L),
                Map.of("k", "v"));
        store.store(name, id, snapshot, null).get(10, TimeUnit.SECONDS);

        Snapshot loaded = store.load(name, id, null).get(10, TimeUnit.SECONDS);
        assertThat(loaded).isNotNull();
        assertThat(loaded.version()).isEqualTo("3");
        assertThat((byte[]) loaded.payload()).isEqualTo("state".getBytes(StandardCharsets.UTF_8));
        assertThat(loaded.metadata()).containsEntry("k", "v");
        assertThat(loaded.position()).isEqualTo(snapshot.position());
    }
}
