package io.kronosdb.connector.axon.event;

import io.kronosdb.connector.axon.KronosDbConfiguration;
import io.kronosdb.connector.axon.KronosDbConnectionManager;
import io.kronosdb.connector.transport.FakeServer;
import io.kronosdb.connector.transport.KronosDbConnectionFactory;
import io.kronosdb.protocol.Append;
import io.kronosdb.protocol.Appended;
import io.kronosdb.protocol.ErrorCode;
import io.kronosdb.protocol.RequestFailed;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.conversion.Converter;
import org.axonframework.eventsourcing.eventstore.AppendCondition;
import org.axonframework.eventsourcing.eventstore.AppendEventsTransactionRejectedException;
import org.axonframework.eventsourcing.eventstore.ConsistencyMarker;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;
import org.axonframework.eventsourcing.eventstore.GenericTaggedEventMessage;
import org.axonframework.eventsourcing.eventstore.GlobalIndexConsistencyMarker;
import org.axonframework.eventsourcing.eventstore.TaggedEventMessage;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.axonframework.messaging.eventhandling.conversion.DelegatingEventConverter;
import org.axonframework.messaging.eventstreaming.Tag;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The append transaction Axon commits, on a scripted server that does not
 * confirm it the first time: what goes out again, and what Axon is told.
 */
class KronosDbEventStorageEngineTest {

    /** Payloads are byte arrays already; the converter hands them back. */
    private static final Converter PASS_THROUGH = new Converter() {
        @SuppressWarnings("unchecked")
        @Override
        public <T> T convert(Object input, Type targetType) {
            return (T) input;
        }

        @Override
        public void describeTo(ComponentDescriptor descriptor) {
        }
    };

    private final List<AutoCloseable> closeables = new CopyOnWriteArrayList<>();

    @AfterEach
    void close() throws Exception {
        for (AutoCloseable c : closeables) {
            c.close();
        }
    }

    @FunctionalInterface
    interface Script {
        void run(FakeServer.Session session) throws Exception;
    }

    private FakeServer server(Script script) throws IOException {
        Consumer<FakeServer.Session> wrapped = session -> {
            try {
                session.handshake();
                script.run(session);
            } catch (IOException ignored) {
                // The client hung up.
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
        FakeServer server = new FakeServer(wrapped);
        closeables.add(server);
        return server;
    }

    private EventStorageEngine engine(FakeServer server, long appendRetryMillis) {
        KronosDbConnectionFactory factory = KronosDbConnectionFactory.forClient("orders", "client-1")
                .host("127.0.0.1")
                .port(server.port())
                .keepAlive(0, 0)
                .appendRetryTimeout(appendRetryMillis)
                .build();
        closeables.add(factory::shutdown);
        return new KronosDbEventStorageEngine(factory.connect("default"), new DelegatingEventConverter(PASS_THROUGH));
    }

    private static TaggedEventMessage<?> event(String name) {
        EventMessage message = new GenericEventMessage(
                UUID.randomUUID().toString(),
                new MessageType(name, "1"),
                "{\"sku\":\"A-1\"}".getBytes(StandardCharsets.UTF_8),
                Map.of("user", "åsa"),
                Instant.ofEpochMilli(1_761_300_000_123L));
        return new GenericTaggedEventMessage<>(message, Set.of(Tag.of("order", "42")));
    }

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    void aCommitAnsweredUnavailableSendsTheSameEventsAgainAndSucceedsOnce() throws Exception {
        List<FakeServer.Session.Incoming> appends = new CopyOnWriteArrayList<>();
        FakeServer server = server(session -> {
            FakeServer.Session.Incoming first = session.next(Append.class);
            appends.add(first);
            session.reply(first.stream(), new RequestFailed(ErrorCode.UNAVAILABLE, "not confirmed in time"));
            FakeServer.Session.Incoming second = session.next(Append.class);
            appends.add(second);
            session.reply(second.stream(), new Appended(7, 2, 9));
            session.next();
        });
        List<TaggedEventMessage<?>> events = List.of(event("OrderPlaced"), event("ItemAdded"));

        EventStorageEngine.AppendTransaction transaction = engine(server, 30_000)
                .appendEvents(AppendCondition.none(), null, events).get(10, TimeUnit.SECONDS);
        Object committed = transaction.commit().get(10, TimeUnit.SECONDS);
        ConsistencyMarker marker = (ConsistencyMarker) transaction.afterCommit(committed).get(10, TimeUnit.SECONDS);

        assertThat(GlobalIndexConsistencyMarker.position(marker)).isEqualTo(9);
        assertThat(appends).hasSize(2);
        assertThat(appends.get(1).body()).isEqualTo(appends.get(0).body());
        assertThat(((Append) appends.get(1).frame()).events())
                .extracting(record -> record.event().identifier())
                .containsExactly(events.get(0).event().identifier(), events.get(1).event().identifier());
    }

    @Test
    void aCommitTheServerNeverConfirmsIsRejectedWithItsLastError() throws Exception {
        AtomicInteger appends = new AtomicInteger();
        FakeServer server = server(session -> {
            while (true) {
                FakeServer.Session.Incoming append = session.next(Append.class);
                session.reply(append.stream(),
                        new RequestFailed(ErrorCode.UNAVAILABLE, "refused " + appends.incrementAndGet()));
            }
        });

        EventStorageEngine.AppendTransaction<?> transaction = engine(server, 300)
                .appendEvents(AppendCondition.none(), null, List.of(event("OrderPlaced"))).get(10, TimeUnit.SECONDS);

        assertThatThrownBy(() -> transaction.commit().get(10, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class)
                .cause()
                .isInstanceOf(AppendEventsTransactionRejectedException.class)
                .satisfies(e -> assertThat(e).hasMessage("refused " + appends.get()));
        assertThat(appends.get()).isGreaterThan(1);
    }

    @Test
    void theConfiguredRetryTimeReachesTheEventStore() throws Exception {
        assertThat(new KronosDbConfiguration().getAppendRetryTimeoutMillis()).isEqualTo(30_000);

        AtomicInteger appends = new AtomicInteger();
        // The platform frames the connection manager sends go unanswered.
        FakeServer server = server(session -> {
            while (true) {
                FakeServer.Session.Incoming in = session.next();
                if (in.frame() instanceof Append) {
                    appends.incrementAndGet();
                    session.reply(in.stream(), new RequestFailed(ErrorCode.UNAVAILABLE, "refused"));
                }
            }
        });
        KronosDbConnectionManager manager = KronosDbConnectionManager.builder()
                .configuration(KronosDbConfiguration.builder()
                        .servers("127.0.0.1:" + server.port())
                        .clientId("client-1")
                        .componentName("orders")
                        .heartbeat(false, 5000, 15000)
                        .appendRetryTimeoutMillis(0)
                        .build())
                .build();
        closeables.add(manager::shutdown);
        EventStorageEngine engine = new KronosDbEventStorageEngine(manager.getConnection(),
                new DelegatingEventConverter(PASS_THROUGH));

        EventStorageEngine.AppendTransaction<?> transaction = engine
                .appendEvents(AppendCondition.none(), null, List.of(event("OrderPlaced"))).get(10, TimeUnit.SECONDS);

        assertThatThrownBy(() -> transaction.commit().get(10, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class)
                .cause()
                .isInstanceOf(AppendEventsTransactionRejectedException.class);
        Thread.sleep(300);
        assertThat(appends.get()).isEqualTo(1);
    }
}
