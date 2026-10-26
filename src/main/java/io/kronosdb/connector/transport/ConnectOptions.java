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
        int maxFrame
) {
}
