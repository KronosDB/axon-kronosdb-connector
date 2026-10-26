package io.kronosdb.connector.transport;

import io.kronosdb.protocol.Append;
import io.kronosdb.protocol.AppendConflict;
import io.kronosdb.protocol.AppendSnapshot;
import io.kronosdb.protocol.Appended;
import io.kronosdb.protocol.Condition;
import io.kronosdb.protocol.Criterion;
import io.kronosdb.protocol.Events;
import io.kronosdb.protocol.Frame;
import io.kronosdb.protocol.GetHead;
import io.kronosdb.protocol.GetPositionAt;
import io.kronosdb.protocol.GetSnapshot;
import io.kronosdb.protocol.GetTags;
import io.kronosdb.protocol.GetTail;
import io.kronosdb.protocol.Head;
import io.kronosdb.protocol.PositionAt;
import io.kronosdb.protocol.Snapshot;
import io.kronosdb.protocol.SnapshotAppended;
import io.kronosdb.protocol.SnapshotResult;
import io.kronosdb.protocol.SnapshottedSource;
import io.kronosdb.protocol.Source;
import io.kronosdb.protocol.StoredRecord;
import io.kronosdb.protocol.Subscribe;
import io.kronosdb.protocol.Tag;
import io.kronosdb.protocol.Tags;
import io.kronosdb.protocol.Tail;
import org.jspecify.annotations.Nullable;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * Channel abstraction for the KronosDB event store operations of the native
 * protocol, for one context. Positions are the protocol's u64 in a
 * {@code long}: unsigned, though in practice they stay below 2^63.
 */
public class EventStoreChannel {

    /** Default number of credits to grant initially and on each refill. */
    private static final long DEFAULT_CREDITS = 500;

    private final Transport transport;
    private final String context;

    EventStoreChannel(Transport transport, String context) {
        this.transport = transport;
        this.context = context;
    }

    // ---------------------------------------------------------------- append

    /**
     * Starts an append transaction. Events are buffered locally and sent as
     * one unary request on {@link AppendTransaction#commit()}; nothing is
     * sent before that.
     */
    public AppendTransaction startAppendTransaction() {
        return new AppendTransaction(this);
    }

    /**
     * Appends events in one request, all or nothing. Completes with the
     * positions and the consistency marker for the next conditional append,
     * or fails with {@link ConsistencyConflictException} when the condition
     * does not hold.
     *
     * @param condition the consistency condition, or {@code null} for none
     */
    public CompletableFuture<Appended> append(@Nullable Condition condition, List<StoredRecord> events) {
        CompletableFuture<Frame> reply = transport.withContext(context,
                (connection, id) -> connection.request(new Append(id, condition, events)));
        CompletableFuture<Appended> result = new CompletableFuture<>();
        reply.whenComplete((frame, error) -> {
            if (error != null) {
                result.completeExceptionally(Failures.unwrap(error));
            } else if (frame instanceof Appended appended) {
                result.complete(appended);
            } else if (frame instanceof AppendConflict conflict) {
                result.completeExceptionally(new ConsistencyConflictException(conflict.conflictingPosition()));
            } else {
                result.completeExceptionally(unexpected(frame, "Appended"));
            }
        });
        return result;
    }

    // ----------------------------------------------------------------- reads

    /**
     * Sources events matching the given criteria (finite stream). The last
     * {@link Events} batch carries the consistency marker.
     *
     * @param from     the first position to read
     * @param criteria criteria to filter events; empty matches every event
     */
    public ResultStream<Events> source(long from, List<Criterion> criteria) {
        ResultStream<Events> results = new ResultStream<>(EventStoreChannel::events);
        open(results, (connection, id) ->
                connection.open(new Source(id, from, criteria, 0), true, results.listener()));
        return results;
    }

    /**
     * Streams events matching the given criteria (infinite live-tailing stream)
     * with credit-based flow control.
     *
     * <p>Opens a subscription with initial credits, then automatically grants
     * more credits as events are consumed to keep the pipeline full.</p>
     *
     * @param fromSequence the first position to read
     * @param criteria     criteria to filter events (may be empty for all events)
     */
    public ResultStream<Events> stream(long fromSequence, List<Criterion> criteria) {
        return stream(fromSequence, criteria, DEFAULT_CREDITS);
    }

    /**
     * Streams events with an explicit credit batch size.
     */
    public ResultStream<Events> stream(long fromSequence, List<Criterion> criteria, long creditBatchSize) {
        // Credits count events: a batch costs its event count, progress costs nothing.
        FlowControlledResultStream<Events> results = new FlowControlledResultStream<>(
                EventStoreChannel::events, creditBatchSize, batch -> batch.events().size());
        // batch_size 0 lets the server pick its default; it never sends more
        // unconsumed events than granted credits, so the effective cap is the credit budget.
        open(results, (connection, id) -> connection.open(
                new Subscribe(id, fromSequence, criteria, List.of(), creditBatchSize, 0, 0),
                false,
                results.listener()));
        return results;
    }

    /**
     * Gets the current head (next position to be assigned) of the event store.
     */
    public CompletableFuture<Long> head() {
        return ask(id -> new GetHead(id), frame -> as(frame, Head.class, "Head").position());
    }

    /**
     * Gets the current tail (first event position) of the event store.
     */
    public CompletableFuture<Long> tail() {
        return ask(id -> new GetTail(id), frame -> as(frame, Tail.class, "Tail").position());
    }

    /**
     * Gets the position of the first event at or after the given timestamp.
     *
     * @param timestampMillis milliseconds since epoch
     * @return the position, or -1 if no event exists at or after the given timestamp
     */
    public CompletableFuture<Long> getSequenceAt(long timestampMillis) {
        return ask(id -> new GetPositionAt(id, timestampMillis), frame -> {
            Long position = as(frame, PositionAt.class, "PositionAt").position();
            return position == null ? -1L : position;
        });
    }

    /**
     * Gets the tags for an event at a specific position.
     */
    public CompletableFuture<List<Tag>> getTags(long position) {
        return ask(id -> new GetTags(id, position), frame -> as(frame, Tags.class, "Tags").tags());
    }

    // ------------------------------------------------------------- snapshots

    /**
     * Appends a snapshot record to the log (ADR-0005). The server stores the
     * state bytes opaquely under the key; a newer snapshot for the same key
     * supersedes older ones by log order.
     *
     * @param position the consistency marker of the read the state was folded from
     * @return the position the snapshot was stored at
     */
    public CompletableFuture<Long> appendSnapshot(ByteBuffer key, ByteBuffer state, long position) {
        return ask(id -> new AppendSnapshot(id, key, state, position),
                frame -> as(frame, SnapshotAppended.class, "SnapshotAppended").position());
    }

    /**
     * Fetches the latest snapshot for a key, without events. Completes with
     * {@code null} on a miss — a miss is always legal.
     */
    public CompletableFuture<@Nullable Snapshot> getSnapshot(ByteBuffer key) {
        return ask(id -> new GetSnapshot(id, key),
                frame -> as(frame, SnapshotResult.class, "SnapshotResult").snapshot());
    }

    /**
     * The fused read: the latest snapshot under the key plus all events
     * matching the criteria after it, one stream, one consistent view. The
     * first element is the {@link SnapshotResult}, then {@link Events}
     * batches follow; the last carries the consistency marker.
     */
    public ResultStream<Frame> snapshottedSource(ByteBuffer key, List<Criterion> criteria) {
        ResultStream<Frame> results = new ResultStream<>(frame ->
                frame instanceof SnapshotResult || frame instanceof Events ? frame : null);
        open(results, (connection, id) ->
                connection.open(new SnapshottedSource(id, criteria, key, 0), true, results.listener()));
        return results;
    }

    // ------------------------------------------------------------------ util

    private <T> CompletableFuture<T> ask(java.util.function.IntFunction<Frame> request, Function<Frame, T> reply) {
        CompletableFuture<Frame> frame = transport.withContext(context,
                (connection, id) -> connection.request(request.apply(id)));
        CompletableFuture<T> result = new CompletableFuture<>();
        frame.whenComplete((f, error) -> {
            if (error != null) {
                result.completeExceptionally(Failures.unwrap(error));
                return;
            }
            try {
                result.complete(reply.apply(f));
            } catch (RuntimeException e) {
                result.completeExceptionally(e);
            }
        });
        return result;
    }

    private interface Opener {
        ClientStream open(NativeConnection connection, int contextId);
    }

    /** Opens the server stream behind {@code results} once the context is bound. */
    private void open(ResultStream<?> results, Opener opener) {
        transport.<Void>withContext(context, (connection, id) -> {
            results.attach(opener.open(connection, id));
            return CompletableFuture.completedFuture(null);
        }).whenComplete((ignored, error) -> {
            if (error != null) {
                results.fail(Failures.unwrap(error));
            }
        });
    }

    // Progress frames only matter to selective subscribers that store a token without events: skipped.
    private static @Nullable Events events(Frame frame) {
        return frame instanceof Events events ? events : null;
    }

    private static <T extends Frame> T as(Frame frame, Class<T> type, String expected) {
        if (type.isInstance(frame)) {
            return type.cast(frame);
        }
        throw unexpected(frame, expected);
    }

    private static TransportException unexpected(Frame frame, String expected) {
        return new TransportException("the server answered with " + frame.getClass().getSimpleName()
                + " instead of " + expected);
    }

    /**
     * Represents an in-progress append transaction. Requests are merged
     * locally; the wire sees a single unary Append at commit time, so a
     * rolled-back transaction sends nothing at all.
     */
    public static class AppendTransaction {

        private final EventStoreChannel channel;
        private @Nullable Condition condition;
        private @Nullable List<StoredRecord> pending;
        private boolean closed;

        AppendTransaction(EventStoreChannel channel) {
            this.channel = channel;
        }

        /**
         * Appends events to this transaction with an optional consistency condition.
         * May be called more than once before commit; events accumulate and the
         * first condition wins.
         */
        public synchronized void append(@Nullable Condition condition, List<StoredRecord> events) {
            if (closed) {
                throw new IllegalStateException("Append transaction already committed or rolled back.");
            }
            if (pending == null) {
                this.condition = condition;
                this.pending = events;
            } else {
                if (this.condition == null && condition != null) {
                    this.condition = condition;
                }
                if (!(pending instanceof ArrayList)) {
                    pending = new ArrayList<>(pending);
                }
                pending.addAll(events);
            }
        }

        /**
         * Commits the transaction by sending the buffered request.
         *
         * @return a future that completes with the append response
         */
        public synchronized CompletableFuture<Appended> commit() {
            if (closed) {
                throw new IllegalStateException("Append transaction already committed or rolled back.");
            }
            closed = true;
            List<StoredRecord> events = pending == null ? List.of() : pending;
            return channel.append(condition, events);
        }

        /**
         * Rolls back the transaction. Nothing was sent, so this only drops
         * the buffered events.
         */
        public synchronized void rollback() {
            closed = true;
            pending = null;
        }
    }
}
