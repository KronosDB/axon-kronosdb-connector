package io.kronosdb.connector.axon.command;

import io.kronosdb.connector.axon.MetadataConverter;
import io.kronosdb.protocol.Command;
import io.kronosdb.protocol.CommandResult;
import io.kronosdb.protocol.MetadataValue;
import io.kronosdb.protocol.Payload;
import io.kronosdb.protocol.ProcessingInstruction;
import io.kronosdb.protocol.ProcessingKey;
import org.axonframework.conversion.Converter;
import org.axonframework.messaging.commandhandling.CommandMessage;
import org.axonframework.messaging.commandhandling.CommandResultMessage;
import org.axonframework.messaging.commandhandling.GenericCommandMessage;
import org.axonframework.messaging.commandhandling.GenericCommandResultMessage;
import org.axonframework.messaging.core.GenericMessage;
import org.axonframework.messaging.core.MessageType;
import org.jspecify.annotations.Nullable;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * Utility class for converting between Axon Framework command messages
 * and KronosDB native protocol commands.
 */
public final class CommandConverter {

    private static final byte[] NO_BYTES = new byte[0];

    private CommandConverter() {
    }

    /**
     * Converts an Axon Framework {@link CommandMessage} to a protocol {@link Command}.
     */
    public static Command convertCommandMessage(CommandMessage command, String clientId, String componentName) {
        Object payload = command.payload();
        if (!(payload instanceof byte[] payloadAsBytes)) {
            throw new IllegalArgumentException(
                    "Payload must be of type byte[] for KronosDB connector, but was: "
                            + command.payloadType().getName());
        }

        List<ProcessingInstruction> instructions = new ArrayList<>(2);
        command.routingKey().ifPresent(routingKey -> instructions.add(
                new ProcessingInstruction(ProcessingKey.ROUTING_KEY, new MetadataValue.OfText(routingKey))));
        command.priority().ifPresent(priority -> instructions.add(
                new ProcessingInstruction(ProcessingKey.PRIORITY, new MetadataValue.OfNumber(priority))));

        return new Command(
                command.identifier(),
                command.type().name(),
                System.currentTimeMillis(),
                new Payload(command.type().name(), command.type().version(), ByteBuffer.wrap(payloadAsBytes)),
                MetadataConverter.toWireMetadata(command.metadata()),
                instructions,
                clientId,
                componentName);
    }

    /**
     * Converts a protocol {@link CommandResult} to an Axon Framework {@link CommandResultMessage}.
     */
    public static CompletableFuture<CommandResultMessage> convertCommandResponse(
            CommandResult commandResponse, @Nullable Converter converter) {
        if (commandResponse.error() != null && !commandResponse.errorCode().isEmpty()) {
            return CompletableFuture.failedFuture(
                    new KronosDbCommandDispatchException(
                            commandResponse.errorCode(),
                            commandResponse.error().message()));
        }

        Payload payload = commandResponse.payload();
        if (payload == null || payload.typeName().isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }

        MessageType messageType = new MessageType(payload.typeName(), payload.revision());
        Map<String, String> metadata = MetadataConverter.fromWireMetadata(commandResponse.metadata());
        return CompletableFuture.completedFuture(new GenericCommandResultMessage(new GenericMessage(
                commandResponse.messageId(),
                messageType,
                toArray(payload.data()),
                metadata
        )).withConverter(converter));
    }

    /**
     * Converts a protocol {@link Command} to an Axon Framework {@link CommandMessage}.
     */
    public static CommandMessage convertCommand(Command command, @Nullable Converter converter) {
        Payload commandPayload = command.payload();
        int priority = priority(command.instructions());
        String routingKey = routingKey(command.instructions());
        return new GenericCommandMessage(
                new GenericMessage(
                        command.messageId(),
                        new MessageType(commandPayload.typeName(), commandPayload.revision()),
                        toArray(commandPayload.data()),
                        MetadataConverter.fromWireMetadata(command.metadata())
                ),
                routingKey,
                priority
        ).withConverter(converter);
    }

    /**
     * Converts an Axon Framework {@link CommandResultMessage} to a protocol {@link CommandResult}.
     */
    public static CommandResult convertResultMessage(@Nullable CommandResultMessage resultMessage,
                                                     String requestIdentifier) {
        if (resultMessage == null) {
            return new CommandResult(UUID.randomUUID().toString(), requestIdentifier, "", null, null,
                    List.of(), List.of());
        }
        Object payload = resultMessage.payload();
        String messageId = Objects.requireNonNullElse(resultMessage.identifier(), UUID.randomUUID().toString());

        if (payload != null && !(payload instanceof byte[])) {
            throw new IllegalArgumentException(
                    "Payload must be of type byte[] for KronosDB connector, but was: "
                            + resultMessage.payloadType().getName());
        }
        byte[] payloadAsBytes = (byte[]) Objects.requireNonNullElse(payload, NO_BYTES);
        return new CommandResult(
                messageId,
                requestIdentifier,
                "",
                null,
                new Payload(resultMessage.type().name(), resultMessage.type().version(),
                        ByteBuffer.wrap(payloadAsBytes)),
                MetadataConverter.toWireMetadata(resultMessage.metadata()),
                List.of());
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

    private static String routingKey(List<ProcessingInstruction> instructions) {
        for (ProcessingInstruction instruction : instructions) {
            if (instruction.key().equals(ProcessingKey.ROUTING_KEY)
                    && instruction.value() instanceof MetadataValue.OfText text) {
                return text.value();
            }
        }
        return "";
    }
}
