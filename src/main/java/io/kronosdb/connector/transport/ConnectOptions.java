package io.kronosdb.connector.transport;

import org.jspecify.annotations.Nullable;

import javax.net.ssl.SSLContext;

/**
 * Everything needed to open a native connection.
 *
 * @param host                 the server's host
 * @param port                 the server's port: the one it listens on for clients and peers
 *                             ({@code --listen}), with or without TLS
 * @param clientName           the application name, shown in the admin console
 * @param clientId             this instance, unique per process
 * @param token                a static bearer token, or {@code null}
 * @param sslContext           the TLS context, or {@code null} for plaintext
 * @param connectTimeoutMillis how long to wait for the TCP connection and the handshake
 * @param keepAliveMillis      ping a connection that has been silent this long; 0 relies on the server's pings
 * @param keepAliveTimeoutMillis how long to wait for any sign of life after such a ping
 * @param maxFrame             the largest frame to send or accept, or 0 for the limit the server announces
 * @param window               each stream's window: the bytes of frames the server may send on a stream ahead of
 *                             the consumer reading it, at least {@link #MIN_WINDOW}
 */
public record ConnectOptions(
        String host,
        int port,
        String clientName,
        String clientId,
        @Nullable String token,
        @Nullable SSLContext sslContext,
        long connectTimeoutMillis,
        long keepAliveMillis,
        long keepAliveTimeoutMillis,
        int maxFrame,
        int window
) {

    /** Each stream's window unless configured otherwise: 8 MiB, enough to fill a fast link a few milliseconds away. */
    public static final int DEFAULT_WINDOW = 8 * 1024 * 1024;
    /** The smallest window the server accepts; anything less would make every stream crawl. */
    public static final int MIN_WINDOW = 64 * 1024;

    public ConnectOptions {
        if (window < MIN_WINDOW) {
            throw new IllegalArgumentException("the stream window must be at least " + MIN_WINDOW
                    + " bytes, not " + window);
        }
    }
}
