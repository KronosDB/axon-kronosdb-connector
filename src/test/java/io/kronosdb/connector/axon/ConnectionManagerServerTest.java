package io.kronosdb.connector.axon;

import io.kronosdb.connector.axon.event.EventProcessorControlService;
import io.kronosdb.connector.transport.RealServer;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.messaging.eventhandling.processing.EventProcessor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static io.kronosdb.connector.transport.RealServer.eventually;
import static io.kronosdb.connector.transport.RealServer.unique;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * The connection manager and the processor control service against a real
 * KronosDB server: an application registering on the platform, and the
 * console controlling its event processors. Needs
 * {@code -Dkronosdb.native.admin}; see {@link RealServer}.
 */
class ConnectionManagerServerTest {

    private KronosDbConnectionManager manager;
    private EventProcessorControlService control;

    @BeforeEach
    void requireServer() {
        RealServer.requireAdmin();
    }

    @AfterEach
    void close() {
        if (control != null) {
            control.shutdown();
        }
        if (manager != null) {
            manager.shutdown();
        }
    }

    private KronosDbConnectionManager manager(String clientId) {
        KronosDbConfiguration configuration = KronosDbConfiguration.builder()
                .servers(RealServer.ADDRESS)
                .clientId(clientId)
                .componentName("orders-service")
                .heartbeat(true, 500, 15000)
                .build();
        manager = KronosDbConnectionManager.builder().configuration(configuration).build();
        return manager;
    }

    private static String clients() throws Exception {
        return RealServer.adminGet("/api/v1/clients").replaceAll("\\s", "");
    }

    private static String processors() throws Exception {
        return RealServer.adminGet("/api/v1/processors").replaceAll("\\s", "");
    }

    @Test
    void startingTheManagerRegistersTheApplicationAndShuttingItDownRemovesIt() throws Exception {
        String clientId = unique("app");
        manager(clientId).start();
        manager.getConnection();

        // Registration lists the client a moment before its platform stream
        // is open, so wait for this client's own entry to say it streams.
        java.util.regex.Pattern streaming = java.util.regex.Pattern.compile(
                "\\{\"client_id\":\"" + clientId + "\",\"component\":\"orders-service\"[^}]*\"streaming\":true");
        try {
            eventually("the application is listed and streaming", 10, () -> streaming.matcher(clients()).find());
        } catch (AssertionError e) {
            throw new AssertionError(e.getMessage() + "; the server lists " + clients(), e);
        }

        manager.shutdown();

        eventually("the application is gone", 10, () -> !clients().contains("\"client_id\":\"" + clientId + "\""));
    }

    @Test
    void theConsoleCanPauseAndStartAnEventProcessorOfTheApplication() throws Exception {
        String clientId = unique("app");
        String name = unique("OrderProjection");
        EventProcessor processor = mock(EventProcessor.class);
        when(processor.name()).thenReturn(name);
        when(processor.shutdown()).thenReturn(CompletableFuture.completedFuture(null));
        when(processor.start()).thenReturn(CompletableFuture.completedFuture(null));
        Configuration configuration = mock(Configuration.class);
        when(configuration.getComponents(EventProcessor.class)).thenReturn(Map.of(name, processor));

        manager(clientId).start();
        control = new EventProcessorControlService(configuration, manager.getConnection().platformChannel());
        eventually("the application is listed", 10, () -> clients().contains(clientId));
        control.start();

        eventually("the processor is listed", 15, () -> processors().contains("\"name\":\"" + name + "\""));

        assertThat(RealServer.adminPost("/api/processors/" + name + "/pause").replaceAll("\\s", ""))
                .contains("\"ok\":true");
        verify(processor, timeout(10_000)).shutdown();

        assertThat(RealServer.adminPost("/api/processors/" + name + "/start").replaceAll("\\s", ""))
                .contains("\"ok\":true");
        verify(processor, timeout(10_000)).start();

        // Still healthy afterwards.
        assertThat(clients()).contains(clientId);
    }
}
