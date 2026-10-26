package io.kronosdb.connector.axon.query;

import io.kronosdb.connector.axon.KronosDbException;
import io.kronosdb.connector.transport.ResultStream;
import io.kronosdb.protocol.QueryResult;
import org.axonframework.conversion.Converter;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.SimpleEntry;
import org.axonframework.messaging.queryhandling.QueryResponseMessage;
import org.jspecify.annotations.Nullable;

import java.util.Optional;

/**
 * A {@link MessageStream} that wraps a KronosDB {@link ResultStream} of {@link QueryResult}
 * messages, converting them to Axon Framework {@link QueryResponseMessage}s.
 */
public class QueryResponseMessageStream implements MessageStream<QueryResponseMessage> {

    private final ResultStream<QueryResult> stream;
    private final @Nullable Converter converter;

    // A handler's failure ends the stream: it is reported through error() rather than thrown by next().
    private volatile @Nullable Throwable failure;

    public QueryResponseMessageStream(ResultStream<QueryResult> stream, @Nullable Converter converter) {
        this.stream = stream;
        this.converter = converter;
    }

    @Override
    public Optional<Entry<QueryResponseMessage>> next() {
        if (failure != null) {
            return Optional.empty();
        }
        QueryResult response = stream.nextIfAvailable();
        if (response == null) {
            return Optional.empty();
        }
        KronosDbException failed = QueryConverter.failureOf(response);
        if (failed != null) {
            failure = failed;
            return Optional.empty();
        }
        return Optional.of(new SimpleEntry<>(QueryConverter.convertQueryResponse(response, converter)));
    }

    @Override
    public Optional<Entry<QueryResponseMessage>> peek() {
        QueryResult response = stream.peek();
        if (failure != null || response == null || QueryConverter.failureOf(response) != null) {
            return Optional.empty();
        }
        return Optional.of(new SimpleEntry<>(QueryConverter.convertQueryResponse(response, converter)));
    }

    @Override
    public void setCallback(Runnable callback) {
        stream.onAvailable(callback);
    }

    @Override
    public Optional<Throwable> error() {
        Throwable failed = failure;
        return failed != null ? Optional.of(failed) : stream.getError();
    }

    @Override
    public boolean isCompleted() {
        return failure != null || stream.isCompleted();
    }

    @Override
    public boolean hasNextAvailable() {
        QueryResult head = stream.peek();
        return failure == null && head != null && QueryConverter.failureOf(head) == null;
    }

    @Override
    public void close() {
        stream.close();
    }
}
