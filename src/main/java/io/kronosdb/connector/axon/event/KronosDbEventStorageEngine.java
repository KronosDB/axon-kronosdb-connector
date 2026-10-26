package io.kronosdb.connector.axon.event;

import io.kronosdb.connector.transport.EventStoreChannel;
import io.kronosdb.connector.transport.Failures;
import io.kronosdb.connector.transport.KronosDbConnection;
import io.kronosdb.connector.transport.ResultStream;
import io.kronosdb.protocol.Appended;
import io.kronosdb.protocol.Criterion;
import io.kronosdb.protocol.Events;
import io.kronosdb.protocol.StoredRecord;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.eventsourcing.eventstore.AppendCondition;
import org.axonframework.eventsourcing.eventstore.AppendEventsTransactionRejectedException;
import org.axonframework.eventsourcing.eventstore.ConsistencyMarker;
import org.axonframework.eventsourcing.eventstore.EmptyAppendTransaction;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;
import org.axonframework.eventsourcing.eventstore.GlobalIndexConsistencyMarker;
import org.axonframework.eventsourcing.eventstore.SourcingCondition;
import org.axonframework.eventsourcing.eventstore.TaggedEventMessage;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.Metadata;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.conversion.EventConverter;
import org.axonframework.messaging.eventhandling.processing.streaming.token.GlobalSequenceTrackingToken;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.axonframework.messaging.eventstreaming.StreamingCondition;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * An {@link EventStorageEngine} implementation backed by KronosDB.
 * <p>
 * Delegates all event store operations to a KronosDB instance over the native protocol.
 */
public class KronosDbEventStorageEngine implements EventStorageEngine {

    private static final Logger logger = LoggerFactory.getLogger(KronosDbEventStorageEngine.class);

    private final KronosDbConnection connection;
    private final TaggedEventConverter converter;

    /**
     * Constructs a {@code KronosDbEventStorageEngine}.
     *
     * @param connection the KronosDB connection to use
     * @param converter  the event converter for serializing/deserializing payloads
     */
    public KronosDbEventStorageEngine(KronosDbConnection connection, EventConverter converter) {
        this.connection = Objects.requireNonNull(connection, "The KronosDB connection cannot be null.");
        this.converter = new TaggedEventConverter(converter);
    }

    @Override
    public CompletableFuture<AppendTransaction<?>> appendEvents(AppendCondition condition,
                                                                 @Nullable ProcessingContext context,
                                                                 List<TaggedEventMessage<?>> events) {
        if (events.isEmpty()) {
            return CompletableFuture.completedFuture(EmptyAppendTransaction.INSTANCE);
        }

        EventStoreChannel.AppendTransaction appendTransaction = eventChannel().startAppendTransaction();

        List<StoredRecord> records = new ArrayList<>(events.size());
        for (TaggedEventMessage<?> event : events) {
            StoredRecord record = converter.convertTaggedEventMessage(event);
            if (logger.isDebugEnabled()) {
                logger.debug("Appending event [{}] with timestamp [{}], tags: {}.",
                        record.event().identifier(),
                        record.event().timestamp(),
                        record.event().tags().stream()
                                .map(t -> ConditionConverter.describeTag(t))
                                .toList());
            }
            records.add(record);
        }

        appendTransaction.append(ConditionConverter.convertAppendCondition(condition), records);

        return CompletableFuture.completedFuture(new KronosDbAppendTransaction(appendTransaction));
    }

    @Override
    public MessageStream<EventMessage> source(SourcingCondition condition, @Nullable ProcessingContext context) {
        if (logger.isDebugEnabled()) {
            logger.debug("Sourcing events with condition [{}].", condition);
        }

        long from = ConditionConverter.sourcingFrom(condition);
        List<Criterion> criteria = ConditionConverter.sourcingCriteria(condition);
        if (logger.isDebugEnabled()) {
            logger.debug("Source request: from={}, criteria={}.",
                    from,
                    criteria.stream().map(ConditionConverter::describe).toList());
        }
        ResultStream<Events> stream = eventChannel().source(from, criteria);
        return new SourcingEventMessageStream(stream, converter);
    }

    @Override
    public MessageStream<EventMessage> stream(StreamingCondition condition) {
        if (logger.isDebugEnabled()) {
            logger.debug("Streaming events with condition [{}].", condition);
        }

        long fromSequence = ConditionConverter.extractStreamFromSequence(condition);
        var criteria = ConditionConverter.extractStreamCriteria(condition);
        ResultStream<Events> stream = eventChannel().stream(fromSequence, criteria);
        return new StreamingEventMessageStream(stream, converter);
    }

    @Override
    public CompletableFuture<TrackingToken> firstToken() {
        logger.debug("Retrieving first token (tail).");
        return eventChannel().tail()
                .thenApply(position -> new GlobalSequenceTrackingToken(position));
    }

    @Override
    public CompletableFuture<TrackingToken> latestToken() {
        logger.debug("Retrieving latest token (head).");
        return eventChannel().head()
                .thenApply(position -> new GlobalSequenceTrackingToken(position));
    }

    @Override
    public CompletableFuture<TrackingToken> tokenAt(Instant at) {
        logger.debug("Retrieving token at timestamp [{}].", at);
        return eventChannel().getSequenceAt(at.toEpochMilli())
                .thenApply(position -> new GlobalSequenceTrackingToken(position == -1 ? 0 : position));
    }

    private EventStoreChannel eventChannel() {
        return connection.eventStoreChannel();
    }

    @Override
    public void describeTo(ComponentDescriptor descriptor) {
        descriptor.describeProperty("connection", connection);
        descriptor.describeProperty("converter", converter);
    }

    private record KronosDbAppendTransaction(
            EventStoreChannel.AppendTransaction appendTransaction
    ) implements AppendTransaction<Appended> {

        @Override
        public CompletableFuture<Appended> commit() {
            logger.debug("Committing append transaction...");
            return appendTransaction.commit()
                    .exceptionallyCompose(throwable -> {
                        Throwable cause = Failures.unwrap(throwable);
                        logger.warn("Append transaction commit failed.", cause);
                        return CompletableFuture.failedFuture(
                                new AppendEventsTransactionRejectedException(cause.getMessage())
                        );
                    });
        }

        @Override
        public CompletableFuture<ConsistencyMarker> afterCommit(Appended appendResponse) {
            long marker = appendResponse.marker();
            logger.debug("Append transaction succeeded with marker [{}].", marker);
            return CompletableFuture.completedFuture(new GlobalIndexConsistencyMarker(marker));
        }

        @Override
        public void rollback() {
            appendTransaction.rollback();
        }
    }
}
