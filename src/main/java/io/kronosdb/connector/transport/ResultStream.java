package io.kronosdb.connector.transport;

import io.kronosdb.protocol.Frame;
import org.jspecify.annotations.Nullable;

import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * An async pull-based result stream backed by a server stream. Provides
 * non-blocking polling and callback-based notification when new items arrive.
 *
 * <p>Frames are queued as they arrive, so the stream never makes the
 * connection wait for a slow consumer. What the server sends ahead of the
 * consumer is bounded by the stream's window, which the stream grants back as
 * elements are taken from it: a slow consumer slows its own stream, is never
 * failed for it, and holds nothing else up.
 *
 * @param <T> the type of elements in this stream
 */
public class ResultStream<T> implements AutoCloseable {

    /** An element with what the window counts for it, granted back when it is taken. */
    private record Queued<T>(T item, int size, StreamWindow window) {
    }

    private final BlockingQueue<Queued<T>> queue = new LinkedBlockingQueue<>();
    private final Function<Frame, T> mapper;
    private final StreamListener listener = new Listener();

    private volatile @Nullable Throwable error;
    private volatile boolean completed;
    private volatile boolean closed;
    private volatile @Nullable Runnable onAvailableCallback;
    private volatile @Nullable ClientStream stream;

    /**
     * @param mapper turns a frame into the element it carries; {@code null} for frames that carry none
     */
    ResultStream(Function<Frame, T> mapper) {
        this.mapper = mapper;
    }

    /** The listener to open the server stream with. */
    StreamListener listener() {
        return listener;
    }

    /** Connects the stream to its server side once it is open. */
    void attach(ClientStream stream) {
        this.stream = stream;
        if (closed) {
            stream.cancel();
        }
    }

    /** The stream could not be opened. */
    void fail(Throwable cause) {
        listener.onFailure(cause);
    }

    /** The server stream, for subclasses that send on it. */
    @Nullable ClientStream clientStream() {
        return stream;
    }

    private final class Listener implements StreamListener.Paced {
        @Override
        public void onFrame(Frame frame, boolean last, int size, StreamWindow window) {
            T item = mapper.apply(frame);
            if (item != null) {
                queue.add(new Queued<>(item, size, window));
            } else if (size > 0) {
                // A frame that carries nothing for the consumer is taken at once.
                window.consumed(size);
            }
            if (last) {
                completed = true;
            }
            signal();
        }

        @Override
        public void onFailure(Throwable cause) {
            error = cause;
            completed = true;
            signal();
        }
    }

    private void signal() {
        Runnable callback = onAvailableCallback;
        if (callback != null) {
            callback.run();
        }
    }

    /**
     * Returns the next available element, or {@code null} if none is immediately available.
     */
    @Nullable
    public T nextIfAvailable() {
        return taken(queue.poll());
    }

    /**
     * Waits up to the specified time for the next element.
     */
    @Nullable
    public T nextIfAvailable(long timeout, TimeUnit unit) throws InterruptedException {
        return taken(queue.poll(timeout, unit));
    }

    private @Nullable T taken(@Nullable Queued<T> queued) {
        if (queued == null) {
            return null;
        }
        queued.window().consumed(queued.size());
        return queued.item();
    }

    /**
     * Peeks at the next element without consuming it.
     */
    @Nullable
    public T peek() {
        Queued<T> queued = queue.peek();
        return queued == null ? null : queued.item();
    }

    /**
     * Returns whether there is at least one element available for immediate consumption.
     */
    public boolean hasNext() {
        return !queue.isEmpty();
    }

    /**
     * Returns whether this stream has been fully consumed (completed and queue empty).
     */
    public boolean isCompleted() {
        return completed && queue.isEmpty();
    }

    /**
     * Returns whether the server has closed its side of the stream.
     */
    public boolean isClosed() {
        return completed;
    }

    /**
     * Returns the error that terminated the stream, if any.
     */
    public Optional<Throwable> getError() {
        return Optional.ofNullable(error);
    }

    /**
     * Registers a callback to be invoked when new elements become available
     * or the stream completes/errors.
     */
    public void onAvailable(Runnable callback) {
        this.onAvailableCallback = callback;
        // If items are already available, fire immediately.
        if (!queue.isEmpty() || completed) {
            callback.run();
        }
    }

    /** Ends the stream: the server is told to stop and what is still queued can be read. */
    @Override
    public void close() {
        closed = true;
        ClientStream s = stream;
        if (s != null) {
            s.cancel();
        }
        completed = true;
    }
}
