package io.kronosdb.connector.axon.snapshot;

import io.kronosdb.connector.transport.Failures;
import io.kronosdb.connector.transport.KronosDbConnection;
import io.kronosdb.connector.transport.RequestFailedException;
import io.kronosdb.protocol.ErrorCode;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.conversion.Converter;
import org.axonframework.eventsourcing.eventstore.AggregateSequenceNumberPosition;
import org.axonframework.eventsourcing.eventstore.GlobalIndexPosition;
import org.axonframework.eventsourcing.eventstore.Position;
import org.axonframework.eventsourcing.snapshot.api.Snapshot;
import org.axonframework.eventsourcing.snapshot.store.SnapshotStore;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.jspecify.annotations.Nullable;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/**
 * A KronosDB-backed implementation of {@link SnapshotStore}.
 * <p>
 * Stores and retrieves aggregate snapshots via the KronosDB EventStore snapshot
 * RPCs (ADR-0005: snapshots ride the replicated event log). KronosDB stores one
 * opaque state blob per key, so this adapter encodes the Axon snapshot's
 * version, timestamp, metadata, position type, and payload into the blob
 * itself; the numeric position value travels in the wire {@code position}
 * field. Snapshots are keyed by qualified name + identifier, matching the
 * Axon Framework convention. Superseded snapshots need no pruning — a newer
 * record for the same key wins by log order.
 */
public class KronosDbSnapshotStore implements SnapshotStore {

    /** Format version of the encoded state blob. */
    private static final byte STATE_FORMAT_V1 = 1;
    private static final byte POSITION_GLOBAL_INDEX = 0;
    private static final byte POSITION_AGGREGATE_SEQUENCE = 1;

    private final KronosDbConnection connection;
    private final Converter converter;

    public KronosDbSnapshotStore(KronosDbConnection connection, Converter converter) {
        this.connection = Objects.requireNonNull(connection);
        this.converter = Objects.requireNonNull(converter);
    }

    private ByteBuffer makeKey(QualifiedName qn, Object identifier) {
        byte[] name = converter.convert(qn.name(), byte[].class);
        byte[] id = converter.convert(identifier, byte[].class);
        byte[] key = new byte[name.length + 1 + id.length];
        System.arraycopy(name, 0, key, 0, name.length);
        // key[name.length] is the NUL separating the two parts.
        System.arraycopy(id, 0, key, name.length + 1, id.length);
        return ByteBuffer.wrap(key);
    }

    @Override
    public CompletableFuture<Void> store(QualifiedName qualifiedName, Object identifier, Snapshot snapshot,
                                         @Nullable ProcessingContext context) {
        Objects.requireNonNull(qualifiedName);
        Objects.requireNonNull(identifier);
        Objects.requireNonNull(snapshot);

        long position = switch (snapshot.position()) {
            case GlobalIndexPosition gip -> GlobalIndexPosition.toIndex(snapshot.position());
            case AggregateSequenceNumberPosition asnp ->
                    AggregateSequenceNumberPosition.toSequenceNumber(snapshot.position());
            default -> throw new IllegalArgumentException("Unsupported position type: " + snapshot.position());
        };

        byte positionType = switch (snapshot.position()) {
            case GlobalIndexPosition gip -> POSITION_GLOBAL_INDEX;
            case AggregateSequenceNumberPosition asnp -> POSITION_AGGREGATE_SEQUENCE;
            default -> throw new IllegalArgumentException("Unsupported position type: " + snapshot.position());
        };

        byte[] payload = converter.convert(snapshot.payload(), byte[].class) instanceof byte[] ba
                ? ba : new byte[0];

        return connection.eventStoreChannel()
                .appendSnapshot(makeKey(qualifiedName, identifier),
                        encodeState(snapshot, positionType, payload),
                        position)
                .thenApply(v -> null);
    }

    @Override
    public CompletableFuture<@Nullable Snapshot> load(QualifiedName qualifiedName, Object identifier,
                                                      @Nullable ProcessingContext context) {
        Objects.requireNonNull(qualifiedName);
        Objects.requireNonNull(identifier);

        return connection.eventStoreChannel()
                .getSnapshot(makeKey(qualifiedName, identifier))
                .thenApply(snapshot -> {
                    if (snapshot == null) {
                        return null;
                    }
                    return decodeState(snapshot.state(), snapshot.position());
                })
                .exceptionally(e -> {
                    e = Failures.unwrap(e);
                    if (e instanceof RequestFailedException failed
                            && (failed.code().equals(ErrorCode.CANCELLED)
                            || failed.code().equals(ErrorCode.NOT_FOUND))) {
                        return null;
                    }
                    throw new CompletionException(
                            "Snapshot loading failed for %s with identifier %s"
                                    .formatted(qualifiedName.toString(), identifier.toString()), e);
                });
    }

    @Override
    public void describeTo(ComponentDescriptor descriptor) {
        descriptor.describeProperty("connection", connection);
        descriptor.describeProperty("converter", converter);
    }

    static ByteBuffer encodeState(Snapshot snapshot, byte positionType, byte[] payload) {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream(payload.length + 128);
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            out.writeByte(STATE_FORMAT_V1);
            out.writeByte(positionType);
            out.writeUTF(snapshot.version());
            out.writeLong(snapshot.timestamp().toEpochMilli());
            out.writeInt(snapshot.metadata().size());
            for (Map.Entry<String, String> entry : snapshot.metadata().entrySet()) {
                out.writeUTF(entry.getKey());
                out.writeUTF(entry.getValue());
            }
            out.writeInt(payload.length);
            out.write(payload);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to encode snapshot state", e);
        }
        return ByteBuffer.wrap(bytes.toByteArray());
    }

    static Snapshot decodeState(ByteBuffer state, long positionValue) {
        byte[] encoded = new byte[state.remaining()];
        state.get(state.position(), encoded);
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(encoded))) {
            byte formatVersion = in.readByte();
            if (formatVersion != STATE_FORMAT_V1) {
                throw new IllegalArgumentException("Unsupported snapshot state format: " + formatVersion);
            }
            byte positionType = in.readByte();
            Position position = switch (positionType) {
                case POSITION_GLOBAL_INDEX -> new GlobalIndexPosition(positionValue);
                case POSITION_AGGREGATE_SEQUENCE -> new AggregateSequenceNumberPosition(positionValue);
                default -> throw new IllegalArgumentException("Unexpected position type: " + positionType);
            };
            String version = in.readUTF();
            Instant timestamp = Instant.ofEpochMilli(in.readLong());
            int metadataCount = in.readInt();
            Map<String, String> metadata = new HashMap<>(metadataCount);
            for (int i = 0; i < metadataCount; i++) {
                metadata.put(in.readUTF(), in.readUTF());
            }
            byte[] payload = new byte[in.readInt()];
            in.readFully(payload);
            return new Snapshot(position, version, payload, timestamp, metadata);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to decode snapshot state", e);
        }
    }
}
