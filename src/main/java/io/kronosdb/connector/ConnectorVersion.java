package io.kronosdb.connector;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * The version of this connector, as built: the project version Maven wrote
 * into {@code version.properties}. A client reports it to the server, which
 * shows it beside the client in the admin console.
 */
public final class ConnectorVersion {

    /** The version, or {@code unknown} when the jar was put together without the properties file. */
    public static final String VERSION = read();

    private ConnectorVersion() {
    }

    private static String read() {
        try (InputStream in = ConnectorVersion.class.getResourceAsStream("version.properties")) {
            if (in == null) {
                return "unknown";
            }
            Properties properties = new Properties();
            properties.load(in);
            return properties.getProperty("version", "unknown");
        } catch (IOException e) {
            return "unknown";
        }
    }
}
