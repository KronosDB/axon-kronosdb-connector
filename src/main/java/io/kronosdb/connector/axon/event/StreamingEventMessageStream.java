package io.kronosdb.connector.axon.event;

import io.kronosdb.connector.grpc.ResultStream;
import io.kronosdb.grpc.eventstore.SequencedEvent;
import io.kronosdb.grpc.eventstore.StreamResponse;
import org.axonframework.messaging.core.Context;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.SimpleEntry;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.processing.streaming.token.GlobalSequenceTrackingToken;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Optional;

/**
 * A {@link MessageStream} backed by a KronosDB stream (infinite live-tailing) event stream.
 * Translates KronosDB {@link StreamResponse} messages to Axon Framework {@link EventMessage}s.
 * <p>
 * This stream remains open and pushes new events as they are appended to KronosDB.
 */
public class StreamingEventMessageStream implements MessageStream<EventMessage> {

    private static final Logger logger = LoggerFactory.getLogger(StreamingEventMessageStream.class);

    private final ResultStream<StreamResponse> stream;
    private final TaggedEventConverter converter;

    public StreamingEventMessageStream(ResultStream<StreamResponse> stream, TaggedEventConverter converter) {
        this.stream = Objects.requireNonNull(stream);
        this.converter = Objects.requireNonNull(converter);
    }

    @Override
    public Optional<Entry<EventMessage>> next() {
        // Skip heartbeat messages — they are server keep-alives, not events.
        while (true) {
            StreamResponse response = stream.nextIfAvailable();
            if (response == null) {
                return Optional.empty();
            }
            if (response.hasEvent()) {
                return Optional.of(convertToEntry(response.getEvent()));
            }
            // Heartbeat or unknown — skip and try next
            logger.trace("Received stream heartbeat, skipping.");
        }
    }

    @Override
    public Optional<Entry<EventMessage>> peek() {
        StreamResponse response = stream.peek();
        if (response == null) {
            return Optional.empty();
        }
        if (!response.hasEvent()) {
            // Consume the heartbeat so next peek/next gets a real event
            stream.nextIfAvailable();
            return Optional.empty();
        }
        return Optional.of(convertToEntry(response.getEvent()));
    }

    private SimpleEntry<EventMessage> convertToEntry(SequencedEvent event) {
        EventMessage eventMessage = converter.convertEvent(event.getEvent());
        TrackingToken token = new GlobalSequenceTrackingToken(event.getSequence() + 1);
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
        return stream.isClosed();
    }

    @Override
    public boolean hasNextAvailable() {
        return stream.peek() != null;
    }

    @Override
    public void close() {
        stream.close();
    }
}
