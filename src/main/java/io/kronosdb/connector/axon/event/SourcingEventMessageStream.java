package io.kronosdb.connector.axon.event;

import io.kronosdb.connector.grpc.ResultStream;
import io.kronosdb.grpc.eventstore.SequencedEvent;
import io.kronosdb.grpc.eventstore.SourceResponse;
import org.axonframework.eventsourcing.eventstore.ConsistencyMarker;
import org.axonframework.eventsourcing.eventstore.GlobalIndexConsistencyMarker;
import org.axonframework.messaging.core.Context;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.SimpleEntry;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.TerminalEventMessage;
import org.axonframework.messaging.eventhandling.processing.streaming.token.GlobalSequenceTrackingToken;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Optional;

/**
 * A {@link MessageStream} backed by a KronosDB source (finite) event stream.
 * Translates KronosDB {@link SourceResponse} messages to Axon Framework {@link EventMessage}s.
 * <p>
 * At the end of the stream, the consistency marker is made available via the
 * {@link ConsistencyMarker#RESOURCE_KEY} resource.
 */
public class SourcingEventMessageStream implements MessageStream<EventMessage> {

    private static final Logger logger = LoggerFactory.getLogger(SourcingEventMessageStream.class);

    private final ResultStream<SourceResponse> stream;
    private final TaggedEventConverter converter;

    public SourcingEventMessageStream(ResultStream<SourceResponse> stream, TaggedEventConverter converter) {
        this.stream = Objects.requireNonNull(stream);
        this.converter = Objects.requireNonNull(converter);
    }

    @Override
    public Optional<Entry<EventMessage>> next() {
        SourceResponse next = stream.nextIfAvailable();
        if (next == null) {
            return Optional.empty();
        } else if (next.hasConsistencyMarker()) {
            logger.debug("Reached consistency marker [{}].", next.getConsistencyMarker());
            return convertToMarkerEntry(next.getConsistencyMarker());
        }
        return convertToEventEntry(next.getEvent());
    }

    @Override
    public Optional<Entry<EventMessage>> peek() {
        SourceResponse peeked = stream.peek();
        if (peeked == null) {
            return Optional.empty();
        } else if (peeked.hasConsistencyMarker()) {
            return convertToMarkerEntry(peeked.getConsistencyMarker());
        }
        return convertToEventEntry(peeked.getEvent());
    }

    private Optional<Entry<EventMessage>> convertToEventEntry(SequencedEvent event) {
        EventMessage eventMessage = converter.convertEvent(event.getEvent());
        TrackingToken token = new GlobalSequenceTrackingToken(event.getSequence() + 1);
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
