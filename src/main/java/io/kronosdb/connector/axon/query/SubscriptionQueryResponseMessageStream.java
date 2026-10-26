package io.kronosdb.connector.axon.query;

import io.kronosdb.connector.axon.KronosDbException;
import io.kronosdb.connector.axon.MetadataConverter;
import io.kronosdb.connector.transport.QueryChannel;
import io.kronosdb.connector.transport.ResultStream;
import io.kronosdb.protocol.SubscriptionEvent;
import org.axonframework.conversion.Converter;
import org.axonframework.messaging.core.GenericMessage;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.SimpleEntry;
import org.axonframework.messaging.queryhandling.GenericQueryResponseMessage;
import org.axonframework.messaging.queryhandling.QueryResponseMessage;
import org.jspecify.annotations.Nullable;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A {@link MessageStream} that wraps a KronosDB subscription query result,
 * converting {@link SubscriptionEvent}s (initial results and updates)
 * to Axon Framework {@link QueryResponseMessage}s.
 */
public class SubscriptionQueryResponseMessageStream implements MessageStream<QueryResponseMessage> {

    private final QueryChannel.SubscriptionQueryResult queryResult;
    private final ResultStream<SubscriptionEvent> stream;
    private final @Nullable Converter converter;

    // Terminal state signalled by the handler via complete / complete_exceptionally.
    private volatile boolean terminated;
    private volatile @Nullable Throwable terminalError;

    // Update flow control: refill the server-side credit in batches as updates are consumed.
    private final long refillBatch;
    private final AtomicLong consumedSinceRefill = new AtomicLong(0);

    public SubscriptionQueryResponseMessageStream(QueryChannel.SubscriptionQueryResult queryResult,
                                                   @Nullable Converter converter) {
        this.queryResult = queryResult;
        this.stream = queryResult.results();
        this.converter = converter;
        this.refillBatch = Math.max(1, queryResult.initialPermits() / 4);
    }

    @Override
    public Optional<Entry<QueryResponseMessage>> next() {
        SubscriptionEvent response = stream.nextIfAvailable();
        if (response == null) {
            return Optional.empty();
        }
        Optional<Entry<QueryResponseMessage>> entry = convertResponse(response);
        if (response instanceof SubscriptionEvent.OfUpdate) {
            onUpdateConsumed();
        }
        return entry;
    }

    private void onUpdateConsumed() {
        if (terminated || stream.isClosed()) {
            return;
        }
        if (consumedSinceRefill.incrementAndGet() >= refillBatch) {
            consumedSinceRefill.addAndGet(-refillBatch);
            queryResult.sendFlowControl(refillBatch);
        }
    }

    @Override
    public Optional<Entry<QueryResponseMessage>> peek() {
        SubscriptionEvent response = stream.peek();
        if (response == null) {
            return Optional.empty();
        }
        return convertResponse(response);
    }

    private Optional<Entry<QueryResponseMessage>> convertResponse(SubscriptionEvent response) {
        switch (response) {
            case SubscriptionEvent.OfInitialResult initial -> {
                KronosDbException failed = QueryConverter.failureOf(initial.value());
                if (failed != null) {
                    terminalError = failed;
                    terminated = true;
                    return Optional.empty();
                }
                return Optional.of(new SimpleEntry<>(
                        QueryConverter.convertQueryResponse(initial.value(), converter)));
            }
            case SubscriptionEvent.OfUpdate update -> {
                // An update's payload becomes a query response message.
                var payload = update.value().payload();
                var message = new GenericMessage(
                        update.value().messageId(),
                        new MessageType(payload.typeName(), payload.revision()),
                        toArray(payload.data()),
                        MetadataConverter.fromWireMetadata(update.value().metadata())
                );
                return Optional.of(new SimpleEntry<>(
                        new GenericQueryResponseMessage(message).withConverter(converter)));
            }
            case SubscriptionEvent.OfComplete ignored -> terminated = true;
            case SubscriptionEvent.OfCompleteExceptionally failed -> {
                var failure = failed.value();
                String message = failure.error() != null ? failure.error().message() : failure.errorCode();
                terminalError = new KronosDbException(failure.errorCode(), message);
                terminated = true;
            }
        }
        return Optional.empty();
    }

    private static byte[] toArray(java.nio.ByteBuffer buffer) {
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(buffer.position(), bytes);
        return bytes;
    }

    @Override
    public void setCallback(Runnable callback) {
        stream.onAvailable(callback);
    }

    @Override
    public Optional<Throwable> error() {
        Throwable error = terminalError;
        if (error != null) {
            return Optional.of(error);
        }
        return stream.getError();
    }

    @Override
    public boolean isCompleted() {
        return terminated || stream.isCompleted();
    }

    @Override
    public boolean hasNextAvailable() {
        return !terminated && stream.peek() != null;
    }

    @Override
    public void close() {
        queryResult.close();
        stream.close();
    }
}
