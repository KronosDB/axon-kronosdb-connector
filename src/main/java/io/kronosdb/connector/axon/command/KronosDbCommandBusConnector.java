package io.kronosdb.connector.axon.command;

import io.kronosdb.connector.axon.KronosDbConfiguration;
import io.kronosdb.connector.transport.KronosDbConnection;
import io.kronosdb.connector.transport.Registration;
import io.kronosdb.protocol.Command;
import io.kronosdb.protocol.CommandResult;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.messaging.commandhandling.CommandMessage;
import org.axonframework.messaging.commandhandling.CommandResultMessage;
import io.kronosdb.connector.axon.distributed.command.CommandBusConnector;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.conversion.MessageConverter;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.Objects.requireNonNull;

/**
 * A {@link CommandBusConnector} implementation that connects to KronosDB
 * for distributed command dispatching and handling.
 */
public class KronosDbCommandBusConnector implements CommandBusConnector {

    private static final Logger logger = LoggerFactory.getLogger(KronosDbCommandBusConnector.class);

    private final KronosDbConnection connection;
    private final String clientId;
    private final String componentName;
    private final @Nullable MessageConverter converter;

    private @Nullable Handler incomingHandler;
    private final Map<QualifiedName, Registration> subscriptions = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, CompletableFuture<?>> commandsInProgress = new ConcurrentHashMap<>();

    public KronosDbCommandBusConnector(KronosDbConnection connection,
                                       KronosDbConfiguration configuration) {
        this(connection, configuration, null);
    }

    public KronosDbCommandBusConnector(KronosDbConnection connection,
                                       KronosDbConfiguration configuration,
                                       @Nullable MessageConverter converter) {
        this.connection = requireNonNull(connection);
        requireNonNull(configuration);
        this.clientId = configuration.getClientId();
        this.componentName = configuration.getComponentName();
        this.converter = converter;
    }

    public void start() {
        logger.trace("KronosDbCommandBusConnector started.");
    }

    @Override
    public CompletableFuture<CommandResultMessage> dispatch(CommandMessage command,
                                                             @Nullable ProcessingContext processingContext) {
        return connection.commandChannel()
                .sendCommand(CommandConverter.convertCommandMessage(command, clientId, componentName))
                .thenCompose(response -> CommandConverter.convertCommandResponse(response, converter));
    }

    @Override
    public CompletableFuture<Void> subscribe(QualifiedName commandName, int loadFactor) {
        logger.debug("Subscribing to command [{}] with load factor [{}].", commandName, loadFactor);

        Registration registration = connection.commandChannel()
                .registerCommandHandler(this::handle, loadFactor, clientId, componentName, commandName.name());

        this.subscriptions.put(commandName, registration);
        CompletableFuture<Void> completion = new CompletableFuture<>();
        registration.onAck(() -> completion.complete(null));
        return completion;
    }

    private CompletableFuture<CommandResult> handle(Command command) {
        logger.debug("Received incoming command [{}].", command.name());
        try {
            CompletableFuture<CommandResult> result = new CompletableFuture<>();
            result.whenComplete((r, e) -> commandsInProgress.remove(command.messageId()));
            commandsInProgress.put(command.messageId(), result);

            requireNonNull(incomingHandler, "incomingHandler not configured")
                    .handle(CommandConverter.convertCommand(command, converter),
                            new FutureResultCallback(result, command));

            return result;
        } catch (Exception e) {
            logger.error("Error processing incoming command: {}", command.name(), e);
            commandsInProgress.remove(command.messageId());
            return CompletableFuture.failedFuture(e);
        }
    }

    @Override
    public boolean unsubscribe(QualifiedName commandName) {
        Registration subscription = subscriptions.remove(commandName);
        if (subscription != null) {
            subscription.cancel();
            return true;
        }
        return false;
    }

    @Override
    public void onIncomingCommand(Handler handler) {
        this.incomingHandler = handler;
    }

    public CompletableFuture<Void> disconnect() {
        if (!connection.isConnected()) {
            return CompletableFuture.completedFuture(null);
        }
        logger.trace("Disconnecting KronosDbCommandBusConnector.");
        return connection.commandChannel().prepareDisconnect()
                .thenCompose(r -> CompletableFuture.allOf(
                        commandsInProgress.values().toArray(CompletableFuture[]::new)));
    }

    public CompletableFuture<Void> shutdownDispatching() {
        logger.trace("Shutting down dispatching.");
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public void describeTo(ComponentDescriptor descriptor) {
        descriptor.describeProperty("connection", connection);
        descriptor.describeProperty("clientId", clientId);
        descriptor.describeProperty("componentName", componentName);
    }

    private record FutureResultCallback(
            CompletableFuture<CommandResult> result,
            Command command
    ) implements ResultCallback {

        @Override
        public void onSuccess(@Nullable CommandResultMessage resultMessage) {
            result.complete(CommandConverter.convertResultMessage(resultMessage, command.messageId()));
        }

        @Override
        public void onError(Throwable cause) {
            logger.info("Command [{}] raised an exception: {}", command.name(), cause.getMessage());
            result.completeExceptionally(cause);
        }
    }
}
