package io.kronosdb.connector.axon.query;

import io.kronosdb.connector.axon.KronosDbException;
import io.kronosdb.connector.axon.MetadataConverter;
import io.kronosdb.protocol.ErrorDetail;
import io.kronosdb.protocol.MetadataValue;
import io.kronosdb.protocol.Payload;
import io.kronosdb.protocol.ProcessingInstruction;
import io.kronosdb.protocol.ProcessingKey;
import io.kronosdb.protocol.Query;
import io.kronosdb.protocol.QueryResult;
import io.kronosdb.protocol.QueryUpdate;
import io.kronosdb.protocol.SubscriptionQuery;
import org.axonframework.conversion.Converter;
import org.axonframework.messaging.core.GenericMessage;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.queryhandling.GenericQueryMessage;
import org.axonframework.messaging.queryhandling.GenericQueryResponseMessage;
import org.axonframework.messaging.queryhandling.GenericSubscriptionQueryUpdateMessage;
import org.axonframework.messaging.queryhandling.QueryMessage;
import org.axonframework.messaging.queryhandling.QueryResponseMessage;
import org.axonframework.messaging.queryhandling.SubscriptionQueryUpdateMessage;
import org.jspecify.annotations.Nullable;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/**
 * Utility class for converting between Axon Framework query messages
 * and KronosDB native protocol queries.
 */
public final class QueryConverter {

    private static final byte[] NO_BYTES = new byte[0];

    private QueryConverter() {
    }

    /**
     * Converts a protocol {@link Query} to an Axon Framework {@link QueryMessage}.
     */
    static QueryMessage convertQueryRequest(Query queryRequest, @Nullable Converter converter) {
        Payload payload = queryRequest.payload();
        Integer priority = priority(queryRequest.instructions());

        var type = new MessageType(payload.typeName(), payload.revision());
        return new GenericQueryMessage(
                new GenericMessage(
                        queryRequest.messageId(),
                        type,
                        toArray(payload.data()),
                        MetadataConverter.fromWireMetadata(queryRequest.metadata())
                ),
                priority
        ).withConverter(converter);
    }

    /**
     * Converts an Axon Framework {@link QueryMessage} to a protocol {@link Query}.
     * The server's query timeout applies.
     */
    public static Query convertQueryMessage(QueryMessage query, String clientId, String componentName) {
        return convertQueryMessage(query, clientId, componentName, 0);
    }

    /**
     * Converts an Axon Framework {@link QueryMessage} to a protocol {@link Query}
     * that may wait {@code timeoutMillis} for its answer; {@code 0} leaves the
     * timeout to the server.
     */
    public static Query convertQueryMessage(QueryMessage query, String clientId, String componentName,
                                            long timeoutMillis) {
        Object payload = query.payload();
        if (!(payload instanceof byte[] payloadAsBytes)) {
            throw new IllegalArgumentException(
                    "Payload must be of type byte[] for KronosDB connector, but was: "
                            + query.payloadType().getName());
        }

        List<ProcessingInstruction> instructions = new ArrayList<>(3);
        query.priority().ifPresent(priority -> instructions.add(
                new ProcessingInstruction(ProcessingKey.PRIORITY, new MetadataValue.OfNumber(priority))));
        instructions.add(new ProcessingInstruction(ProcessingKey.NR_OF_RESULTS, new MetadataValue.OfNumber(1)));
        // Axon's own connector always sent one hour, a placeholder; without an
        // explicit timeout the server's [timeouts] query applies.
        if (timeoutMillis > 0) {
            instructions.add(new ProcessingInstruction(ProcessingKey.TIMEOUT,
                    new MetadataValue.OfNumber(timeoutMillis)));
        }

        return new Query(
                query.identifier(),
                query.type().name(),
                System.currentTimeMillis(),
                new Payload(query.type().name(), query.type().version(), ByteBuffer.wrap(payloadAsBytes)),
                MetadataConverter.toWireMetadata(query.metadata()),
                instructions,
                clientId,
                componentName);
    }

    /**
     * The failure a handler reported in {@code queryResponse}, or {@code null} if it is an answer.
     */
    public static @Nullable KronosDbException failureOf(QueryResult queryResponse) {
        if (queryResponse.errorCode().isEmpty() && queryResponse.error() == null) {
            return null;
        }
        String message = queryResponse.error() != null
                ? queryResponse.error().message()
                : queryResponse.errorCode();
        return new KronosDbException(queryResponse.errorCode(), message);
    }

    /**
     * The result a handler sends when its query failed with {@code error}.
     */
    public static QueryResult convertQueryFailure(String requestId, Throwable error) {
        String message = error.getMessage() != null ? error.getMessage() : error.toString();
        return new QueryResult(
                UUID.randomUUID().toString(),
                requestId,
                "QUERY_EXECUTION_ERROR",
                new ErrorDetail(message, "", List.of(), "QUERY_EXECUTION_ERROR"),
                null,
                List.of(),
                List.of());
    }

    /**
     * Converts a protocol {@link QueryResult} to an Axon Framework {@link QueryResponseMessage}.
     * The result must be an answer, not a failure: see {@link #failureOf(QueryResult)}.
     */
    public static QueryResponseMessage convertQueryResponse(QueryResult queryResponse,
                                                            @Nullable Converter converter) {
        if (failureOf(queryResponse) != null) {
            throw new IllegalArgumentException("Query response contained an error.");
        }
        Payload responsePayload = queryResponse.payload();
        var message = new GenericMessage(
                queryResponse.messageId(),
                new MessageType(responsePayload.typeName(), responsePayload.revision()),
                toArray(responsePayload.data()),
                MetadataConverter.fromWireMetadata(queryResponse.metadata())
        );
        return new GenericQueryResponseMessage(message).withConverter(converter);
    }

    /**
     * Converts an Axon Framework {@link QueryResponseMessage} to a protocol {@link QueryResult}.
     */
    public static QueryResult convertQueryResponseMessage(String requestId,
                                                          QueryResponseMessage queryResponseMessage) {
        byte[] payload = Objects.requireNonNullElseGet(
                queryResponseMessage.payloadAs(byte[].class), () -> NO_BYTES);
        return new QueryResult(
                queryResponseMessage.identifier(),
                requestId,
                "",
                null,
                new Payload(queryResponseMessage.type().name(), queryResponseMessage.type().version(),
                        ByteBuffer.wrap(payload)),
                MetadataConverter.toWireMetadata(queryResponseMessage.metadata()),
                List.of());
    }

    /**
     * Converts a {@link SubscriptionQuery} to a {@link QueryMessage}.
     */
    public static QueryMessage convertSubscriptionQueryMessage(SubscriptionQuery query,
                                                               @Nullable Converter converter) {
        Payload responsePayload = query.query().payload();
        var message = new GenericMessage(
                query.subscriptionId(),
                new MessageType(responsePayload.typeName(), responsePayload.revision()),
                toArray(responsePayload.data()),
                MetadataConverter.fromWireMetadata(query.query().metadata())
        );
        return new GenericQueryMessage(message).withConverter(converter);
    }

    /**
     * Converts a {@link SubscriptionQueryUpdateMessage} to a {@link QueryUpdate}.
     */
    public static QueryUpdate convertQueryUpdate(SubscriptionQueryUpdateMessage update,
                                                 String clientId, String componentName) {
        byte[] payload = Objects.requireNonNullElseGet(update.payloadAs(byte[].class), () -> NO_BYTES);
        return new QueryUpdate(
                update.identifier(),
                new Payload(update.type().name(), update.type().version(), ByteBuffer.wrap(payload)),
                MetadataConverter.toWireMetadata(update.metadata()),
                clientId,
                componentName,
                "",
                null);
    }

    /**
     * Converts a {@link QueryUpdate} to a {@link SubscriptionQueryUpdateMessage}.
     */
    public static SubscriptionQueryUpdateMessage convertQueryUpdate(QueryUpdate queryUpdate,
                                                                    @Nullable Converter converter) {
        Payload payload = queryUpdate.payload();
        var message = new GenericMessage(
                queryUpdate.messageId(),
                new MessageType(payload.typeName(), payload.revision()),
                toArray(payload.data()),
                MetadataConverter.fromWireMetadata(queryUpdate.metadata())
        );
        return new GenericSubscriptionQueryUpdateMessage(message).withConverter(converter);
    }

    private static byte[] toArray(ByteBuffer buffer) {
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(buffer.position(), bytes);
        return bytes;
    }

    private static int priority(List<ProcessingInstruction> instructions) {
        for (ProcessingInstruction instruction : instructions) {
            if (instruction.key().equals(ProcessingKey.PRIORITY)
                    && instruction.value() instanceof MetadataValue.OfNumber number) {
                return (int) number.value();
            }
        }
        return 0;
    }
}
