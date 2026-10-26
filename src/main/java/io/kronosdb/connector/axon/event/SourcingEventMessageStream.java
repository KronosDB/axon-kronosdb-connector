package io.kronosdb.connector.axon.event;

import io.kronosdb.connector.transport.ResultStream;
import io.kronosdb.protocol.EventRecord;
import io.kronosdb.protocol.Events;
import io.kronosdb.protocol.StoredRecord;
import org.axonframework.eventsourcing.eventstore.ConsistencyMarker;
import org.axonframework.eventsourcing.eventstore.GlobalIndexConsistencyMarker;
import org.axonframework.messaging.core.Context;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.SimpleEntry;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.eventsourcing.eventstore.TerminalEventMessage;
import org.axonframework.messaging.eventhandling.processing.streaming.token.GlobalSequenceTrackingToken;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Objects;
import java.util.Optional;

/**
 * A {@link MessageStream} backed by a KronosDB source (finite) event stream.
 * Translates KronosDB {@link Events} batches to Axon Framework {@link EventMessage}s.
 * <p>
 * The server sends events in batches; this stream
 * unpacks each batch into an internal buffer and hands events out one at a time,
 * preserving the framework's pull-based per-event contract.
 * <p>
 * At the end of the stream, the consistency marker is made available via the
 * {@link ConsistencyMarker#RESOURCE_KEY} resource.
 */
public class SourcingEventMessageStream implements MessageStream<EventMessage> {

    private static final Logger logger = LoggerFactory.getLogger(SourcingEventMessageStream.class);

    private final ResultStream<Events> stream;
    private final TaggedEventConverter converter;

    /** Events from the current batch not yet handed out. */
    private final Deque<StoredRecord> buffer = new ArrayDeque<>();
    /** The end-of-stream consistency marker, once seen; delivered exactly once by next(). */
    private Long pendingMarker;
    private boolean markerDelivered;

    public SourcingEventMessageStream(ResultStream<Events> stream, TaggedEventConverter converter) {
        this.stream = Objects.requireNonNull(stream);
        this.converter = Objects.requireNonNull(converter);
    }

    /**
     * Pulls responses from the underlying stream until the buffer has an event,
     * the marker is seen, or no message is available without blocking.
     */
    private void fill() {
        while (buffer.isEmpty() && pendingMarker == null) {
            Events next = stream.nextIfAvailable();
            if (next == null) {
                return;
            }
            buffer.addAll(next.events());
            // The final batch of the stream carries the marker.
            if (next.marker() != null) {
                logger.debug("Reached consistency marker [{}].", next.marker());
                pendingMarker = next.marker();
            }
        }
    }

    @Override
    public Optional<Entry<EventMessage>> next() {
        fill();
        StoredRecord event = buffer.pollFirst();
        if (event != null) {
            return convertToEventEntry(event);
        }
        if (pendingMarker != null && !markerDelivered) {
            markerDelivered = true;
            return convertToMarkerEntry(pendingMarker);
        }
        return Optional.empty();
    }

    @Override
    public Optional<Entry<EventMessage>> peek() {
        fill();
        StoredRecord event = buffer.peekFirst();
        if (event != null) {
            return convertToEventEntry(event);
        }
        if (pendingMarker != null && !markerDelivered) {
            return convertToMarkerEntry(pendingMarker);
        }
        return Optional.empty();
    }

    private Optional<Entry<EventMessage>> convertToEventEntry(StoredRecord record) {
        EventRecord event = record.event();
        EventMessage eventMessage = converter.convertEvent(event);
        TrackingToken token = new GlobalSequenceTrackingToken(event.position() + 1);
        Context context = Context.with(TrackingToken.RESOURCE_KEY, token);
        return Optional.of(new SimpleEntry<>(eventMessage, context));
    }

    private static Optional<Entry<EventMessage>> convertToMarkerEntry(long marker) {
        Context context = ConsistencyMarker.addToContext(
                Context.empty(), new GlobalIndexConsistencyMarker(marker)
        );
        return Optional.of(new SimpleEntry<>(TerminalEventMessage.INSTANCE, context));
    }

    @Override
    public void setCallback(Runnable callback) {
        stream.onAvailable(callback);
    }

    @Override
    public Optional<Throwable> error() {
        return stream.getError();
    }

    @Override
    public boolean isCompleted() {
        // Completed means nothing is left anywhere: not in the stream's queue, not in the
        // buffer, and the marker has been handed out.
        return stream.isCompleted() && buffer.isEmpty() && (pendingMarker == null || markerDelivered);
    }

    @Override
    public boolean hasNextAvailable() {
        fill();
        return !buffer.isEmpty() || (pendingMarker != null && !markerDelivered);
    }

    @Override
    public void close() {
        stream.close();
    }
}
