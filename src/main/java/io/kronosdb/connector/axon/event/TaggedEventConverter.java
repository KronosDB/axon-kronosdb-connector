package io.kronosdb.connector.axon.event;

import io.kronosdb.protocol.EventRecord;
import io.kronosdb.protocol.MetadataEntry;
import io.kronosdb.protocol.StoredRecord;
import io.kronosdb.protocol.Tag;
import org.axonframework.eventsourcing.eventstore.TaggedEventMessage;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.Metadata;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.axonframework.messaging.eventhandling.conversion.EventConverter;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Converts between Axon Framework {@link TaggedEventMessage} and the native protocol's
 * {@link StoredRecord}.
 * <p>
 * Used during append (framework to wire) and source/stream (wire to framework) operations.
 */
public class TaggedEventConverter {

    private static final byte[] NO_BYTES = new byte[0];

    private final EventConverter converter;

    public TaggedEventConverter(EventConverter converter) {
        this.converter = Objects.requireNonNull(converter, "The EventConverter cannot be null.");
    }

    /**
     * Converts an Axon Framework tagged event to an event record to append. The position is 0:
     * the server assigns it.
     */
    public StoredRecord convertTaggedEventMessage(TaggedEventMessage<?> taggedEvent) {
        EventMessage event = taggedEvent.event();
        return new StoredRecord(0, new EventRecord(
                0,
                event.identifier(),
                event.type().name(),
                event.type().version(),
                event.timestamp().toEpochMilli(),
                convertMetadata(event.metadata()),
                convertTags(taggedEvent.tags()),
                convertPayload(event)));
    }

    private ByteBuffer convertPayload(EventMessage eventMessage) {
        byte[] bytes = eventMessage.payloadAs(byte[].class, converter);
        // Wrapped, not copied: the writer copies it once into the frame.
        return ByteBuffer.wrap(bytes == null ? NO_BYTES : bytes);
    }

    private static List<MetadataEntry> convertMetadata(Metadata metadata) {
        if (metadata.isEmpty()) {
            return List.of();
        }
        List<MetadataEntry> entries = new ArrayList<>(metadata.size());
        metadata.forEach((key, value) -> entries.add(new MetadataEntry(key, value)));
        return entries;
    }

    private static List<Tag> convertTags(Set<org.axonframework.messaging.eventstreaming.Tag> tags) {
        return convertTagSet(tags);
    }

    static List<Tag> convertTagSet(Set<org.axonframework.messaging.eventstreaming.Tag> tags) {
        if (tags.isEmpty()) {
            return List.of();
        }
        List<Tag> converted = new ArrayList<>(tags.size());
        for (org.axonframework.messaging.eventstreaming.Tag tag : tags) {
            converted.add(new Tag(utf8(tag.key()), utf8(tag.value())));
        }
        return converted;
    }

    private static ByteBuffer utf8(String s) {
        return ByteBuffer.wrap(s.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Converts an event record to an Axon Framework event message (for sourcing/streaming).
     */
    public EventMessage convertEvent(EventRecord event) {
        return new GenericEventMessage(
                event.identifier(),
                new MessageType(event.name(), event.version()),
                toArray(event.payload()),
                convertMetadata(event.metadata()),
                Instant.ofEpochMilli(event.timestamp())
        ).withConverter(converter);
    }

    private static Map<String, String> convertMetadata(List<MetadataEntry> entries) {
        if (entries.isEmpty()) {
            return Map.of();
        }
        Map<String, String> metadata = new HashMap<>(entries.size() * 2);
        for (MetadataEntry entry : entries) {
            metadata.put(entry.key(), entry.value());
        }
        return metadata;
    }

    /** The payload as an array: the one copy a decoded event needs, since Axon wants a {@code byte[]}. */
    private static byte[] toArray(ByteBuffer buffer) {
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(buffer.position(), bytes);
        return bytes;
    }
}
