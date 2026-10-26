package io.kronosdb.connector.transport;

import io.kronosdb.protocol.ErrorCode;
import io.kronosdb.protocol.EventRecord;
import io.kronosdb.protocol.StoredRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;

import static io.kronosdb.connector.transport.RealServer.await;
import static io.kronosdb.connector.transport.RealServer.command;
import static io.kronosdb.connector.transport.RealServer.ok;
import static io.kronosdb.connector.transport.RealServer.unique;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Credentials against a real KronosDB server that has authentication on:
 * which requests a token's roles allow, and what the client sees when they do
 * not. Runs when {@code -Dkronosdb.native.auth.addr=host:port} names such a
 * server and the tokens it knows are given as {@code kronosdb.native.auth.full},
 * {@code kronosdb.native.auth.reader} (role {@code read}) and
 * {@code kronosdb.native.auth.writer} (role {@code write}); skipped otherwise.
 * The tokens are test values for a throwaway server, e.g.
 *
 * <pre>
 * KRONOSDB_ACCESS_TOKEN=full-token kronosdb-server --config auth.toml ...
 *
 * [[security.tokens]]
 * name = "reader"
 * token = "reader-token"
 * roles = ["read"]
 *
 * [[security.tokens]]
 * name = "writer"
 * token = "writer-token"
 * roles = ["write"]
 * </pre>
 */
class AuthNativeServerTest {

    private static final String ADDRESS = System.getProperty("kronosdb.native.auth.addr");
    private static final String FULL = System.getProperty("kronosdb.native.auth.full");
    private static final String READER = System.getProperty("kronosdb.native.auth.reader");
    private static final String WRITER = System.getProperty("kronosdb.native.auth.writer");

    private final List<Transport> transports = new CopyOnWriteArrayList<>();

    @BeforeEach
    void requireServer() {
        assumeTrue(ADDRESS != null && FULL != null && READER != null && WRITER != null,
                "set -Dkronosdb.native.auth.addr, .full, .reader and .writer to run against a secured server");
    }

    @AfterEach
    void close() {
        transports.forEach(Transport::close);
    }

    private Transport transport(String token) {
        int colon = ADDRESS.lastIndexOf(':');
        Transport transport = new Transport(new ConnectOptions(ADDRESS.substring(0, colon),
                Integer.parseInt(ADDRESS.substring(colon + 1)), "auth-test", unique("client"), token, null,
                5000, 10_000, 5000, 0));
        transports.add(transport);
        return transport;
    }

    private static void assertFailsWith(ErrorCode code, org.junit.jupiter.api.function.ThrowingSupplier<?> call) {
        assertThatThrownBy(call::get).satisfiesAnyOf(
                e -> assertThat(e).isInstanceOfSatisfying(RequestFailedException.class,
                        f -> assertThat(f.code()).isEqualTo(code)),
                e -> assertThat(e).isInstanceOf(ExecutionException.class).cause()
                        .isInstanceOfSatisfying(RequestFailedException.class,
                                f -> assertThat(f.code()).isEqualTo(code)));
    }

    private static StoredRecord event() {
        return new StoredRecord(0, new EventRecord(0, UUID.randomUUID().toString(), "AuthProbe", "1",
                System.currentTimeMillis(), List.of(), List.of(), ByteBuffer.wrap(new byte[]{1})));
    }

    @Test
    void aClientWithoutACredentialIsRefused() {
        assertFailsWith(ErrorCode.UNAUTHENTICATED, () -> new EventStoreChannel(transport(null), "default").head().get());
    }

    @Test
    void aWrongTokenIsRefused() {
        assertFailsWith(ErrorCode.UNAUTHENTICATED,
                () -> new EventStoreChannel(transport("not-a-token"), "default").head().get());
    }

    @Test
    void theFullTokenDoesEverything() throws Exception {
        String name = unique("Secured");
        Transport handler = transport(FULL);
        await(new CommandChannel(handler, "default", null).registerCommandHandler(
                c -> CompletableFuture.completedFuture(ok(c.messageId(), "ok")), 10, unique("h"), "svc", name)
                .ackFuture());
        Transport full = transport(FULL);

        assertThat(await(new EventStoreChannel(full, "default").append(null, List.of(event())))).isNotNull();
        for (int attempt = 0; attempt < 100; attempt++) {
            try {
                assertThat(await(new CommandChannel(full, "default", null).sendCommand(command(name, "x"))).errorCode())
                        .isEmpty();
                return;
            } catch (ExecutionException e) {
                Thread.sleep(50);
            }
        }
        throw new AssertionError("the command never got through");
    }

    @Test
    void aReadTokenReadsButNeitherWritesNorDispatchesCommandsNorHandlesThem() throws Exception {
        Transport reader = transport(READER);
        EventStoreChannel store = new EventStoreChannel(reader, "default");

        assertThat(await(store.head())).isNotNegative();
        assertFailsWith(ErrorCode.PERMISSION_DENIED, () -> store.append(null, List.of(event())).get());
        assertFailsWith(ErrorCode.PERMISSION_DENIED,
                () -> new CommandChannel(reader, "default", null).sendCommand(command(unique("Any"), "x")).get());

        // Dispatching a query needs only read: the refusal is that nobody handles it.
        ResultStream<io.kronosdb.protocol.QueryResult> results =
                new QueryChannel(reader, "default", null).query(RealServer.query(unique("Any"), "?"));
        RealServer.eventually("the query is answered", 10, results::isCompleted);
        assertThat(results.getError()).hasValueSatisfying(error -> assertThat(error)
                .isInstanceOfSatisfying(RequestFailedException.class,
                        e -> assertThat(e.code()).isEqualTo(ErrorCode.UNAVAILABLE)));
    }

    @Test
    void aWriteTokenAppendsAndDispatchesCommands() throws Exception {
        Transport writer = transport(WRITER);

        assertThat(await(new EventStoreChannel(writer, "default").append(null, List.of(event())))).isNotNull();
        // Nobody handles it, but the dispatch itself is allowed.
        assertFailsWith(ErrorCode.UNAVAILABLE,
                () -> new CommandChannel(writer, "default", null).sendCommand(command(unique("Any"), "x")).get());
    }
}
