package io.kronosdb.connector;

import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

class ConnectorVersionTest {

    @Test
    void theVersionIsTheProjectVersion() throws Exception {
        // The project's own <version> is the first one in the pom.
        Matcher version = Pattern.compile("<version>([^<]+)</version>").matcher(Files.readString(Path.of("pom.xml")));
        assertThat(version.find()).isTrue();

        assertThat(ConnectorVersion.VERSION).isEqualTo(version.group(1));
    }
}
