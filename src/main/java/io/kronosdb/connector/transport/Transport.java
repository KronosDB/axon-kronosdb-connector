package io.kronosdb.connector.transport;

import io.kronosdb.protocol.Frame;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Owns the one native connection of a connector and replaces it when it is
 * lost: the next use after a failure opens a new connection, so callers see
 * an error for the requests that were in flight and nothing else. Everything
 * on a {@link KronosDbConnectionFactory} shares one transport.
 */
public final class Transport implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(Transport.class);

    /** After a failed attempt to connect, calls fail fast for this long instead of retrying each time. */
    private static final long RETRY_AFTER_NANOS = 250_000_000L;

    private final ConnectOptions options;
    private final Object lock = new Object();
    private final ExecutorService handlers = Executors.newThreadPerTaskExecutor(
            Thread.ofVirtual().name("kronosdb-handler-", 0).factory());

    private volatile NativeConnection current;
    private volatile boolean closed;
    private long failedAt;
    private RuntimeException lastFailure;

    public Transport(ConnectOptions options) {
        this.options = options;
    }

    /** A connection to use now: the current one if it is healthy, otherwise a new one. */
    public NativeConnection connection() {
        NativeConnection connection = current;
        if (connection != null && connection.isUsable()) {
            return connection;
        }
        synchronized (lock) {
            if (closed) {
                throw new ConnectionClosedException("the KronosDB connection factory has been shut down");
            }
            connection = current;
            if (connection != null && connection.isUsable()) {
                return connection;
            }
            if (lastFailure != null && System.nanoTime() - failedAt < RETRY_AFTER_NANOS) {
                throw lastFailure;
            }
            try {
                logger.info("Connecting to KronosDB at [{}:{}].", options.host(), options.port());
                connection = NativeConnection.open(options);
            } catch (RuntimeException e) {
                lastFailure = e;
                failedAt = System.nanoTime();
                throw e;
            }
            lastFailure = null;
            NativeConnection previous = current;
            current = connection;
            if (previous != null && !previous.isClosed()) {
                // It asked to be replaced (GoAway); streams on it finish there.
                logger.debug("Replacing a draining KronosDB connection.");
            }
            return connection;
        }
    }

    /** Where the work of handling server-initiated requests (commands, queries) runs. */
    public ExecutorService handlers() {
        return handlers;
    }

    /** Whether the transport was shut down: nothing reconnects after that. */
    public boolean isClosed() {
        return closed;
    }

    /**
     * Runs a reconnect loop on its own virtual thread. Shutting the transport
     * down interrupts it; if that already happened there is nothing to do.
     */
    void background(Runnable loop) {
        try {
            handlers.execute(loop);
        } catch (java.util.concurrent.RejectedExecutionException e) {
            // Shut down meanwhile.
        }
    }

    @FunctionalInterface
    public interface ContextCall<T> {
        CompletableFuture<T> apply(NativeConnection connection, int contextId);
    }

    @FunctionalInterface
    public interface BusCall<T> {
        CompletableFuture<T> apply(NativeConnection connection, int busId);
    }

    /** Runs {@code call} on the live connection with the id the context is bound to there. */
    public <T> CompletableFuture<T> withContext(String context, ContextCall<T> call) {
        NativeConnection connection;
        CompletableFuture<Integer> id;
        try {
            connection = connection();
            id = connection.context(context);
            if (id.isDone() && !id.isCompletedExceptionally()) {
                return call.apply(connection, id.join());
            }
        } catch (RuntimeException e) {
            return CompletableFuture.failedFuture(e);
        }
        NativeConnection bound = connection;
        return id.thenCompose(contextId -> call.apply(bound, contextId));
    }

    /** Runs {@code call} on the live connection with the id the bus is bound to; {@code null} is the default bus. */
    public <T> CompletableFuture<T> withBus(@Nullable String bus, BusCall<T> call) {
        NativeConnection connection;
        CompletableFuture<Integer> id;
        try {
            connection = connection();
            id = bus == null || bus.isEmpty() ? CompletableFuture.completedFuture(0) : connection.bus(bus);
            if (id.isDone() && !id.isCompletedExceptionally()) {
                return call.apply(connection, id.join());
            }
        } catch (RuntimeException e) {
            return CompletableFuture.failedFuture(e);
        }
        NativeConnection bound = connection;
        return id.thenCompose(busId -> call.apply(bound, busId));
    }

    /** Sends a request on the live connection and maps the reply. */
    public <T> CompletableFuture<T> request(Frame request, java.util.function.Function<Frame, T> reply) {
        NativeConnection connection;
        try {
            connection = connection();
        } catch (RuntimeException e) {
            return CompletableFuture.failedFuture(e);
        }
        return connection.request(request).thenApply(reply);
    }

    @Override
    public void close() {
        synchronized (lock) {
            closed = true;
            NativeConnection connection = current;
            current = null;
            if (connection != null) {
                connection.close();
            }
        }
        handlers.shutdownNow();
    }
}
