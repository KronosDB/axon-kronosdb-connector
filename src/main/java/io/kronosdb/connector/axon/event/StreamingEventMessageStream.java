package io.kronosdb.connector.axon.event;

import io.kronosdb.connector.transport.ResultStream;
import io.kronosdb.protocol.EventRecord;
import io.kronosdb.protocol.Events;
import io.kronosdb.protocol.StoredRecord;
import org.axonframework.messaging.core.Context;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.SimpleEntry;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.processing.streaming.token.GlobalSequenceTrackingToken;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Objects;
import java.util.Optional;

/**
 * A {@link MessageStream} backed by a KronosDB stream (infinite live-tailing) event stream.
 * Translates KronosDB {@link Events} batches to Axon Framework {@link EventMessage}s.
 * <p>
 * The server sends events in batches; this stream unpacks each batch into an internal
 * buffer and hands events out one at a time. Progress frames never reach it: the
 * channel skips them.
 * <p>
 * This stream remains open and pushes new events as they are appended to KronosDB.
 */
public class StreamingEventMessageStream implements MessageStream<EventMessage> {

    private final ResultStream<Events> stream;
    private final TaggedEventConverter converter;

    /** Events from the current batch not yet handed out. */
    private final Deque<StoredRecord> buffer = new ArrayDeque<>();

    public StreamingEventMessageStream(ResultStream<Events> stream, TaggedEventConverter converter) {
        this.stream = Objects.requireNonNull(stream);
        this.converter = Objects.requireNonNull(converter);
    }

    /**
     * Pulls batches until the buffer has an event or no batch is available
     * without blocking.
     */
    private void fill() {
        while (buffer.isEmpty()) {
            Events batch = stream.nextIfAvailable();
            if (batch == null) {
                return;
            }
            buffer.addAll(batch.events());
        }
    }

    @Override
    public Optional<Entry<EventMessage>> next() {
        fill();
        StoredRecord event = buffer.pollFirst();
        return event == null ? Optional.empty() : Optional.of(convertToEntry(event));
    }

    @Override
    public Optional<Entry<EventMessage>> peek() {
        fill();
        StoredRecord event = buffer.peekFirst();
        return event == null ? Optional.empty() : Optional.of(convertToEntry(event));
    }

    private SimpleEntry<EventMessage> convertToEntry(StoredRecord record) {
        EventRecord event = record.event();
        EventMessage eventMessage = converter.convertEvent(event);
        TrackingToken token = new GlobalSequenceTrackingToken(event.position() + 1);
        Context context = Context.with(TrackingToken.RESOURCE_KEY, token);
        return new SimpleEntry<>(eventMessage, context);
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
        return stream.isCompleted() && buffer.isEmpty();
    }

    @Override
    public boolean hasNextAvailable() {
        fill();
        return !buffer.isEmpty();
    }

    @Override
    public void close() {
        stream.close();
    }
}
