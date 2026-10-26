package io.kronosdb.protocol;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.zip.CRC32C;
import org.junit.jupiter.api.Test;

/**
 * Pins the wire format this repository was built against.
 *
 * <p>The codec in {@code src/main/java/io/kronosdb/protocol} is copied from a
 * KronosDB checkout by {@code scripts/vendor-protocol.sh} and never edited
 * here. {@link #PINNED_SCHEMA_HASH} is the one line to change when the wire
 * does: updating it is the review point for "the protocol changed", and
 * {@link ConformanceTest} then proves the new codec matches the new fixtures
 * byte for byte.
 */
class SchemaHashTest {

    /** CRC32C of {@code protocol/kronosdb.schema}, as printed by {@code scripts/vendor-protocol.sh}. */
    private static final String PINNED_SCHEMA_HASH = "a65e0eff";

    private static final Path SCHEMA = Path.of(System.getProperty("schema.file", "protocol/kronosdb.schema"));

    @Test
    void theVendoredCodecWasGeneratedFromThePinnedSchema() {
        assertEquals(PINNED_SCHEMA_HASH, Frames.SCHEMA_HASH,
                "the codec in src/main was generated from another schema; run scripts/vendor-protocol.sh "
                        + "and pin the hash it prints");
    }

    @Test
    void theVendoredSchemaIsThePinnedSchema() throws IOException {
        CRC32C crc = new CRC32C();
        crc.update(Files.readAllBytes(SCHEMA));
        assertEquals(PINNED_SCHEMA_HASH, String.format("%08x", crc.getValue()),
                "protocol/kronosdb.schema is not the schema the pinned hash names; run scripts/vendor-protocol.sh");
    }
}
