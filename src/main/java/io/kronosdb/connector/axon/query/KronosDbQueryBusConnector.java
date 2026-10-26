package io.kronosdb.connector.axon.query;

import io.kronosdb.connector.axon.KronosDbConfiguration;
import io.kronosdb.connector.transport.KronosDbConnection;
import io.kronosdb.connector.transport.QueryChannel;
import io.kronosdb.connector.transport.Registration;
import io.kronosdb.connector.transport.ResultStream;
import io.kronosdb.protocol.Query;
import io.kronosdb.protocol.QueryResult;
import io.kronosdb.protocol.QueryUpdate;
import io.kronosdb.protocol.SubscriptionQuery;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.conversion.MessageConverter;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.queryhandling.QueryMessage;
import org.axonframework.messaging.queryhandling.QueryResponseMessage;
import org.axonframework.messaging.queryhandling.SubscriptionQueryUpdateMessage;
import io.kronosdb.connector.axon.distributed.query.QueryBusConnector;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.Objects.requireNonNull;

/**
 * A {@link QueryBusConnector} implementation that connects to KronosDB
 * for distributed query dispatching and handling.
 */
public class KronosDbQueryBusConnector implements QueryBusConnector {

    private static final Logger logger = LoggerFactory.getLogger(KronosDbQueryBusConnector.class);

    private final KronosDbConnection connection;
    private final String clientId;
    private final String componentName;
    private final long queryTimeoutMillis;
    private final @Nullable MessageConverter converter;

    private final Map<QualifiedName, Registration> subscriptions = new ConcurrentHashMap<>();
    private Handler incomingHandler;

    public KronosDbQueryBusConnector(KronosDbConnection connection,
                                     KronosDbConfiguration configuration) {
        this(connection, configuration, null);
    }

    public KronosDbQueryBusConnector(KronosDbConnection connection,
                                     KronosDbConfiguration configuration,
                                     @Nullable MessageConverter converter) {
        this.connection = requireNonNull(connection);
        requireNonNull(configuration);
        this.clientId = configuration.getClientId();
        this.componentName = configuration.getComponentName();
        this.queryTimeoutMillis = configuration.getQueryTimeoutMillis();
        this.converter = converter;
    }

    public void start() {
        logger.trace("KronosDbQueryBusConnector started.");
    }

    @Override
    public CompletableFuture<Void> subscribe(QualifiedName name) {
        logger.debug("Subscribing to query handler [{}].", name);

        Registration registration = connection.queryChannel()
                .registerQueryHandler(
                        new LocalSegmentAdapter(),
                        name.fullName(),
                        "",
                        clientId,
                        componentName
                );

        this.subscriptions.put(name, registration);
        CompletableFuture<Void> completion = new CompletableFuture<>();
        registration.onAck(() -> completion.complete(null));
        return completion;
    }

    @Override
    public boolean unsubscribe(QualifiedName name) {
        Registration subscription = subscriptions.remove(name);
        if (subscription != null) {
            subscription.cancel();
            return true;
        }
        return false;
    }

    @Override
    public void onIncomingQuery(Handler handler) {
        this.incomingHandler = requireNonNull(handler);
    }

    @Override
    public MessageStream<QueryResponseMessage> query(QueryMessage query, @Nullable ProcessingContext context) {
        ResultStream<QueryResult> resultStream = connection.queryChannel()
                .query(QueryConverter.convertQueryMessage(query, clientId, componentName, queryTimeoutMillis));
        return new QueryResponseMessageStream(resultStream, converter);
    }

    @Override
    public MessageStream<QueryResponseMessage> subscriptionQuery(QueryMessage query,
                                                                  @Nullable ProcessingContext context,
                                                                  int updateBufferSize) {
        QueryChannel.SubscriptionQueryResult result = connection.queryChannel()
                .subscriptionQuery(
                        QueryConverter.convertQueryMessage(query, clientId, componentName, queryTimeoutMillis),
                        updateBufferSize
                );
        return new SubscriptionQueryResponseMessageStream(result, converter);
    }

    public CompletableFuture<Void> disconnect() {
        if (connection.isConnected()) {
            logger.trace("Disconnecting KronosDbQueryBusConnector.");
            connection.queryChannel().prepareDisconnect();
        }
        return CompletableFuture.completedFuture(null);
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

    /**
     * Adapts between the KronosDB query handler interface and
     * the Axon Framework query handler.
     */
    private class LocalSegmentAdapter implements QueryChannel.QueryHandler {

        @Override
        public void handle(Query query,
                           java.util.function.Consumer<QueryResult> responseSender,
                           Runnable onComplete) {
            // Runs on one of the transport's virtual threads, so waiting for the answers is fine here.
            MessageStream<QueryResponseMessage> result = null;
            try {
                var queryMessage = QueryConverter.convertQueryRequest(query, converter);
                result = incomingHandler.query(queryMessage);
                Semaphore available = new Semaphore(0);
                result.setCallback(available::release);
                while (!result.isCompleted()) {
                    var next = result.next();
                    if (next.isPresent()) {
                        responseSender.accept(QueryConverter.convertQueryResponseMessage(
                                query.messageId(), next.get().message()));
                    } else {
                        // The callback wakes this; the timeout covers a signal that came too early.
                        available.tryAcquire(10, TimeUnit.MILLISECONDS);
                    }
                }
                Optional<Throwable> error = result.error();
                if (error.isPresent()) {
                    logger.info("Query [{}] failed: {}", query.name(), error.get().getMessage());
                    responseSender.accept(QueryConverter.convertQueryFailure(query.messageId(), error.get()));
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                logger.error("Error handling query [{}].", query.name(), e);
                responseSender.accept(QueryConverter.convertQueryFailure(query.messageId(), e));
            } finally {
                onComplete.run();
                if (result != null) {
                    result.close();
                }
            }
        }

        @Override
        public Registration registerSubscriptionQuery(SubscriptionQuery query,
                                                      QueryChannel.SubscriptionUpdateSender updateSender) {
            var queryMessage = QueryConverter.convertSubscriptionQueryMessage(query, converter);

            var registration = incomingHandler.registerUpdateHandler(
                    queryMessage,
                    new KronosDbUpdateCallback(updateSender)
            );

            return new Registration(() -> {
                registration.cancel();
                updateSender.complete(clientId, componentName);
            });
        }
    }

    /**
     * Bridges Axon Framework's {@link UpdateCallback} to KronosDB's update sender.
     * When projections emit updates via {@code QueryUpdateEmitter}, this callback
     * converts them to protocol frames and sends them back through the handler stream.
     */
    private class KronosDbUpdateCallback implements UpdateCallback {

        private final QueryChannel.SubscriptionUpdateSender updateSender;

        KronosDbUpdateCallback(QueryChannel.SubscriptionUpdateSender updateSender) {
            this.updateSender = updateSender;
        }

        @Override
        public CompletableFuture<Void> sendUpdate(SubscriptionQueryUpdateMessage update) {
            QueryUpdate wireUpdate = QueryConverter.convertQueryUpdate(update, clientId, componentName);
            updateSender.sendUpdate(wireUpdate);
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<Void> complete() {
            updateSender.complete(clientId, componentName);
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<Void> completeExceptionally(Throwable error) {
            updateSender.completeExceptionally(clientId, componentName, "QUERY_EXECUTION_ERROR",
                    error.getMessage() != null ? error.getMessage() : "Unknown error");
            return CompletableFuture.completedFuture(null);
        }
    }
}
