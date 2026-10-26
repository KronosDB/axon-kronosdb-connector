package io.kronosdb.connector.transport;

import io.kronosdb.protocol.Command;
import io.kronosdb.protocol.CommandResult;
import io.kronosdb.protocol.Payload;
import io.kronosdb.protocol.Query;
import io.kronosdb.protocol.QueryResult;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * What the real-server messaging tests share: where the server is, how to
 * reach its admin API, and small builders for the messages they send.
 *
 * <p>The tests run when {@code -Dkronosdb.native.addr=host:port} names a
 * server, and are skipped otherwise. Two more properties unlock tests that
 * need to know more about that server:
 *
 * <ul>
 *   <li>{@code -Dkronosdb.native.admin=host:port}: its admin HTTP API, which the
 *       platform tests use to see registered clients and processors and to send
 *       processor instructions.</li>
 *   <li>{@code -Dkronosdb.native.short-timeouts=true}: it was started with
 *       {@code --command-timeout 3 --query-timeout 3 --heartbeat-interval 1
 *       --heartbeat-timeout 4}, which the timeout tests rely on.</li>
 * </ul>
 *
 * Every test names its commands and queries uniquely, so tests share a server
 * with each other and with anything else on it.
 */
public final class RealServer {

    public static final String ADDRESS = System.getProperty("kronosdb.native.addr");
    public static final String ADMIN = System.getProperty("kronosdb.native.admin");
    public static final boolean SHORT_TIMEOUTS = Boolean.getBoolean("kronosdb.native.short-timeouts");

    private static final HttpClient HTTP = HttpClient.newHttpClient();

    private RealServer() {
    }

    public static void require() {
        assumeTrue(ADDRESS != null, "set -Dkronosdb.native.addr=host:port to run against a server");
    }

    public static void requireAdmin() {
        require();
        assumeTrue(ADMIN != null, "set -Dkronosdb.native.admin=host:port to reach the admin API");
    }

    public static void requireShortTimeouts() {
        require();
        assumeTrue(SHORT_TIMEOUTS, "needs a server started with the short timeouts, see RealServer");
    }

    public static String host() {
        return ADDRESS.substring(0, ADDRESS.lastIndexOf(':'));
    }

    public static int port() {
        return Integer.parseInt(ADDRESS.substring(ADDRESS.lastIndexOf(':') + 1));
    }

    public static ConnectOptions options(String clientId) {
        return new ConnectOptions(host(), port(), "real-server-test", clientId, null, null, 5000, 10_000, 5000, 0);
    }

    /** A transport of its own, as one application instance has. */
    public static Transport transport(String clientId) {
        return new Transport(options(clientId));
    }

    public static String unique(String prefix) {
        return prefix + "-" + UUID.randomUUID().toString().substring(0, 8);
    }

    // ------------------------------------------------------------- messages

    public static Payload payload(String type, String data) {
        return new Payload(type, "1", ByteBuffer.wrap(data.getBytes(StandardCharsets.UTF_8)));
    }

    public static String text(Payload payload) {
        return text(payload.data());
    }

    public static String text(ByteBuffer data) {
        return StandardCharsets.UTF_8.decode(data.duplicate()).toString();
    }

    public static Command command(String name, String body) {
        return new Command(UUID.randomUUID().toString(), name, System.currentTimeMillis(), payload(name, body),
                List.of(), List.of(), "dispatcher", "tests");
    }

    public static Query query(String name, String body) {
        return new Query(UUID.randomUUID().toString(), name, System.currentTimeMillis(), payload(name, body),
                List.of(), List.of(), "dispatcher", "tests");
    }

    public static CommandResult ok(String requestId, String body) {
        return new CommandResult(UUID.randomUUID().toString(), requestId, "", null, payload("Result", body),
                List.of(), List.of());
    }

    public static QueryResult answer(String requestId, String body) {
        return new QueryResult(UUID.randomUUID().toString(), requestId, "", null, payload("Answer", body),
                List.of(), List.of());
    }

    // ------------------------------------------------------------ waiting

    /** Runs {@code work} on a thread of its own, so a test never depends on a shared pool's free workers. */
    public static <T> CompletableFuture<T> async(java.util.concurrent.Callable<T> work) {
        CompletableFuture<T> future = new CompletableFuture<>();
        Thread.startVirtualThread(() -> {
            try {
                future.complete(work.call());
            } catch (Throwable e) {
                future.completeExceptionally(e);
            }
        });
        return future;
    }

    public static <T> T await(CompletableFuture<T> future) throws Exception {
        return future.get(15, TimeUnit.SECONDS);
    }

    /** A condition a test polls for; it may throw. */
    @FunctionalInterface
    public interface Condition {
        boolean holds() throws Exception;
    }

    /** Polls until {@code condition} holds, or fails after {@code seconds}. */
    public static void eventually(String what, int seconds, Condition condition) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(seconds);
        while (!condition.holds()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("timed out waiting for: " + what);
            }
            Thread.sleep(25);
        }
    }

    /** Reads a query's results to its end. */
    public static List<QueryResult> drain(ResultStream<QueryResult> results) throws Exception {
        List<QueryResult> all = new ArrayList<>();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
        while (!results.isCompleted()) {
            QueryResult next = results.nextIfAvailable(50, TimeUnit.MILLISECONDS);
            if (next != null) {
                all.add(next);
            }
            if (System.nanoTime() > deadline) {
                throw new AssertionError("the query did not end; got " + all.size() + " results");
            }
        }
        return all;
    }

    // --------------------------------------------------------------- admin

    public static String adminGet(String path) throws IOException, InterruptedException {
        return admin(HttpRequest.newBuilder(URI.create("http://" + ADMIN + path)).GET());
    }

    public static String adminPost(String path) throws IOException, InterruptedException {
        return admin(HttpRequest.newBuilder(URI.create("http://" + ADMIN + path))
                .POST(HttpRequest.BodyPublishers.noBody()));
    }

    private static String admin(HttpRequest.Builder request) throws IOException, InterruptedException {
        return HTTP.send(request.timeout(Duration.ofSeconds(5)).build(), HttpResponse.BodyHandlers.ofString()).body();
    }
}
