package io.kronosdb.connector.axon;

import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.eventsourcing.configuration.EventSourcingConfigurer;
import org.axonframework.messaging.commandhandling.GenericCommandMessage;
import org.axonframework.messaging.commandhandling.GenericCommandResultMessage;
import org.axonframework.messaging.commandhandling.configuration.CommandHandlingModule;
import org.axonframework.messaging.commandhandling.gateway.CommandGateway;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.axonframework.messaging.eventhandling.SimpleEventHandlingComponent;
import org.axonframework.messaging.eventhandling.gateway.EventGateway;
import org.axonframework.messaging.queryhandling.GenericQueryMessage;
import org.axonframework.messaging.queryhandling.GenericSubscriptionQueryUpdateMessage;
import org.axonframework.messaging.queryhandling.QueryBus;
import org.axonframework.messaging.queryhandling.QueryResponseMessage;
import org.axonframework.messaging.queryhandling.GenericQueryResponseMessage;
import org.axonframework.messaging.queryhandling.configuration.QueryHandlingModule;
import org.axonframework.messaging.queryhandling.gateway.QueryGateway;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static io.kronosdb.connector.transport.RealServer.eventually;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Application smoke test: boots a real Axon Framework application whose
 * infrastructure is wired purely by {@link KronosDbConfigurationEnhancer} (ServiceLoader), then pushes a command,
 * a query and an event through the framework against a live KronosDB.
 */
class AxonApplicationSmokeServerTest {

    private static final String ADDRESS = System.getProperty("kronosdb.native.addr");

    @Test
    void commandsQueriesAndEventsFlowThroughAnAxonApplication() throws Exception {
        assumeTrue(ADDRESS != null, "set -Dkronosdb.native.addr=host:port to run against a server");

        String id = UUID.randomUUID().toString().substring(0, 8);
        QualifiedName ping = new QualifiedName("smoke.Ping" + id);
        QualifiedName echo = new QualifiedName("smoke.Echo" + id);
        QualifiedName happened = new QualifiedName("smoke.Happened" + id);
        List<String> seen = new CopyOnWriteArrayList<>();

        KronosDbConfiguration kronos = KronosDbConfiguration.builder()
                .servers(ADDRESS)
                .componentName("smoke-" + id)
                .clientId("smoke-" + id)
                .build();

        AxonConfiguration app = EventSourcingConfigurer.create()
                .componentRegistry(r -> r.registerComponent(KronosDbConfiguration.class, c -> kronos))
                .registerCommandHandlingModule(CommandHandlingModule.named("smoke-commands")
                        .commandHandlers()
                        .commandHandler(ping, (cmd, ctx) -> MessageStream.just(new GenericCommandResultMessage(
                                new MessageType("smoke.Pong"), "pong:" + cmd.payloadAs(String.class)))))
                .registerQueryHandlingModule(QueryHandlingModule.named("smoke-queries")
                        .queryHandlers()
                        .queryHandler(echo, (query, ctx) -> MessageStream.just(new GenericQueryResponseMessage(
                                new MessageType("smoke.Echoed"), "echo:" + query.payloadAs(String.class)))))
                .messaging(m -> m.eventProcessing(ep -> ep.pooledStreaming(ps -> ps.processor("smoke-" + id,
                        components -> components.eventHandlingComponents(c -> c.declarative("smoke-events", cfg ->
                                SimpleEventHandlingComponent.create("smoke-events")
                                        .subscribe(happened, (event, ctx) -> {
                                            seen.add(event.payloadAs(String.class));
                                            return MessageStream.empty();
                                        }))).notCustomized()))))
                .build();
        app.start();
        try {
            // Command: gateway -> DistributedCommandBus -> KronosDB -> back into the local handler.
            Object pong = app.getComponent(CommandGateway.class)
                    .send(new GenericCommandMessage(new MessageType(ping.name()), "hello"))
                    .getResultMessage().get(15, TimeUnit.SECONDS).payloadAs(String.class);
            assertThat(pong).isEqualTo("pong:hello");

            // Query: gateway -> DistributedQueryBus -> KronosDB scatter-gather -> local handler.
            String echoed = app.getComponent(QueryGateway.class)
                    .query(new GenericQueryMessage(new MessageType(echo.fullName()), "hi"), String.class)
                    .get(15, TimeUnit.SECONDS);
            assertThat(echoed).isEqualTo("echo:hi");

            // Subscription query: initial result, then updates emitted through the QueryBus.
            QueryBus queryBus = app.getComponent(QueryBus.class);
            MessageStream<QueryResponseMessage> subscription = queryBus.subscriptionQuery(
                    new GenericQueryMessage(new MessageType(echo.fullName()), "sub"), null, 16);
            List<String> received = new ArrayList<>();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
            while (received.size() < 2 && System.nanoTime() < deadline) {
                var next = subscription.next();
                if (next.isPresent()) {
                    received.add(String.valueOf(next.get().message().payloadAs(String.class)));
                } else {
                    if (!received.isEmpty()) {
                        // The connector registers for updates before it sends the initial result, so an update
                        // emitted earlier may overtake it. Emit only once the initial result is in. The server
                        // registers the update handler asynchronously, so keep emitting until one lands.
                        queryBus.emitUpdate(q -> true, () -> new GenericSubscriptionQueryUpdateMessage(
                                new MessageType("smoke.Update"), "update-" + 1), null);
                    }
                    Thread.sleep(100);
                }
            }
            subscription.close();
            assertThat(received).hasSizeGreaterThanOrEqualTo(2);
            assertThat(received.get(0)).isEqualTo("echo:sub");
            assertThat(received.get(1)).startsWith("update-");

            // Event: EventGateway -> EventStore -> KronosDB append, then a streaming processor receives it.
            app.getComponent(EventGateway.class)
                    .publish(null, new GenericEventMessage(new MessageType(happened.name()), "it-happened"))
                    .get(15, TimeUnit.SECONDS);
            eventually("the processor saw the event", 20, () -> seen.contains("it-happened"));
        } finally {
            app.shutdown();
        }
    }
}
