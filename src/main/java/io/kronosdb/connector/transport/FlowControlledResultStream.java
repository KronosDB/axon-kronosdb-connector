package io.kronosdb.connector.transport;

import io.kronosdb.protocol.Credit;
import io.kronosdb.protocol.Frame;
import org.jspecify.annotations.Nullable;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.ToLongFunction;

/**
 * A {@link ResultStream} with automatic credit-based flow control.
 *
 * <p>Tracks how many credits the consumed items actually cost and grants
 * more to the server when the remaining count drops below a threshold.
 * Credits count EVENTS, not frames: a batch costs one credit per contained
 * event and a progress frame costs nothing, so the cost is computed per item
 * via {@code creditCost}.</p>
 *
 * @param <T> the type of elements in this stream
 */
public class FlowControlledResultStream<T> extends ResultStream<T> {

    private static final double REFILL_THRESHOLD = 0.25;

    private final long creditBatchSize;
    private final AtomicLong remainingCredits;
    private final ToLongFunction<T> creditCost;

    FlowControlledResultStream(Function<Frame, T> mapper, long creditBatchSize, ToLongFunction<T> creditCost) {
        super(mapper);
        this.creditBatchSize = creditBatchSize;
        this.remainingCredits = new AtomicLong(creditBatchSize);
        this.creditCost = creditCost;
    }

    @Override
    @Nullable
    public T nextIfAvailable() {
        T item = super.nextIfAvailable();
        if (item != null) {
            onConsumed(creditCost.applyAsLong(item));
        }
        return item;
    }

    @Override
    @Nullable
    public T nextIfAvailable(long timeout, TimeUnit unit) throws InterruptedException {
        T item = super.nextIfAvailable(timeout, unit);
        if (item != null) {
            onConsumed(creditCost.applyAsLong(item));
        }
        return item;
    }

    private void onConsumed(long cost) {
        if (cost <= 0) {
            return; // Empty batches consume no credits.
        }
        long remaining = remainingCredits.addAndGet(-cost);
        long threshold = (long) (creditBatchSize * REFILL_THRESHOLD);
        ClientStream stream = clientStream();
        if (remaining <= threshold && !isClosed() && stream != null) {
            long grant = creditBatchSize - remaining;
            remainingCredits.addAndGet(grant);
            try {
                stream.send(new Credit(grant));
            } catch (RuntimeException e) {
                // The connection went away; the stream fails on its own.
            }
        }
    }
}
