package io.kronosdb.connector.axon;

import io.kronosdb.protocol.MessageMetadata;
import io.kronosdb.protocol.MetadataValue;
import org.jspecify.annotations.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Utility class to convert between the native protocol's typed metadata
 * values and the simple String key-value maps used by Axon Framework messages.
 */
public final class MetadataConverter {

    private MetadataConverter() {
    }

    /**
     * Converts a map of String key-value pairs to a list of protocol metadata entries.
     */
    public static List<MessageMetadata> toWireMetadata(Map<String, @Nullable String> source) {
        if (source.isEmpty()) {
            return List.of();
        }
        List<MessageMetadata> result = new ArrayList<>(source.size());
        source.forEach((key, value) -> {
            if (value != null) {
                result.add(new MessageMetadata(key, new MetadataValue.OfText(value)));
            }
        });
        return result;
    }

    /**
     * Converts protocol metadata entries to a map of String key-value pairs.
     */
    public static Map<String, String> fromWireMetadata(List<MessageMetadata> source) {
        Map<String, String> result = new HashMap<>();
        for (MessageMetadata entry : source) {
            String converted = convertFromMetadataValue(entry.value());
            if (converted != null) {
                result.put(entry.key(), converted);
            }
        }
        return result;
    }

    @Nullable
    private static String convertFromMetadataValue(MetadataValue value) {
        return switch (value) {
            case MetadataValue.OfText text -> text.value();
            case MetadataValue.OfDouble number -> Double.toString(number.value());
            case MetadataValue.OfNumber number -> Long.toString(number.value());
            case MetadataValue.OfBoolean bool -> Boolean.toString(bool.value());
            case MetadataValue.OfObject ignored -> null;
        };
    }
}
