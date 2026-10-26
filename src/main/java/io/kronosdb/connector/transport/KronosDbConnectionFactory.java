package io.kronosdb.connector.transport;

import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import javax.net.ssl.TrustManagerFactory;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Factory for creating {@link KronosDbConnection} instances.
 *
 * <p>All connections of a factory share one native TCP connection to the
 * server (opened on first use, replaced when lost); a connection per context
 * is a handle that names the context on it.
 */
public class KronosDbConnectionFactory {

    private static final Logger logger = LoggerFactory.getLogger(KronosDbConnectionFactory.class);
    private static final int DEFAULT_PORT = 50051;

    private final String clientId;
    private final String componentName;
    private final @Nullable String busName;
    private final Transport transport;

    private final Map<String, KronosDbConnection> connections = new ConcurrentHashMap<>();

    private KronosDbConnectionFactory(Builder builder) {
        this.clientId = builder.clientId;
        this.componentName = builder.componentName;
        this.busName = builder.busName;
        this.transport = new Transport(new ConnectOptions(
                builder.host,
                builder.port,
                builder.componentName,
                builder.clientId,
                builder.token,
                builder.sslContext,
                builder.connectTimeoutMillis,
                builder.keepAliveTimeMillis,
                builder.keepAliveTimeoutMillis,
                builder.maxFrame));
    }

    /**
     * Creates a new builder for configuring a connection factory.
     */
    public static Builder forClient(String componentName, String clientId) {
        return new Builder(componentName, clientId);
    }

    /**
     * Creates or retrieves a connection for the given context.
     */
    public KronosDbConnection connect(String context) {
        return connections.computeIfAbsent(context, this::createConnection);
    }

    private KronosDbConnection createConnection(String context) {
        logger.info("Creating KronosDB connection for context [{}].", context);
        return new KronosDbConnection(transport, context, busName);
    }

    /**
     * Shuts down all connections.
     */
    public void shutdown() {
        connections.values().forEach(KronosDbConnection::disconnect);
        connections.clear();
        transport.close();
    }

    public String clientId() {
        return clientId;
    }

    public String componentName() {
        return componentName;
    }

    /**
     * A TLS context that trusts the certificates in a PEM file, in addition to
     * nothing else: the server's certificate (or its CA) is the trust anchor.
     */
    public static SSLContext trusting(File certFile) throws SSLException {
        try (InputStream in = new FileInputStream(certFile)) {
            KeyStore store = KeyStore.getInstance(KeyStore.getDefaultType());
            store.load(null, null);
            int n = 0;
            for (Certificate certificate : CertificateFactory.getInstance("X.509").generateCertificates(in)) {
                store.setCertificateEntry("kronosdb-" + n++, certificate);
            }
            if (n == 0) {
                throw new SSLException("no certificates in " + certFile);
            }
            TrustManagerFactory trust = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            trust.init(store);
            SSLContext context = SSLContext.getInstance("TLS");
            context.init(null, trust.getTrustManagers(), null);
            return context;
        } catch (GeneralSecurityException | IOException e) {
            if (e instanceof SSLException ssl) {
                throw ssl;
            }
            throw new SSLException("could not load the certificate file " + certFile + ": " + e.getMessage(), e);
        }
    }

    /**
     * Builder for {@link KronosDbConnectionFactory}.
     */
    public static class Builder {

        private String host = "localhost";
        private int port = DEFAULT_PORT;
        private final String clientId;
        private final String componentName;
        private @Nullable String busName;
        private @Nullable String token;
        private @Nullable SSLContext sslContext;
        private long connectTimeoutMillis = 10_000;
        private long keepAliveTimeMillis = 10_000;   // 10s ping interval, conventional for long-lived streams
        private long keepAliveTimeoutMillis = 5_000;
        private int maxFrame = 0;

        Builder(String componentName, String clientId) {
            this.componentName = Objects.requireNonNull(componentName);
            this.clientId = Objects.requireNonNull(clientId);
        }

        public Builder host(String host) {
            this.host = Objects.requireNonNull(host);
            return this;
        }

        /**
         * The server's port: the one it listens on for clients and peers
         * ({@code --listen}), with or without TLS.
         */
        public Builder port(int port) {
            this.port = port;
            return this;
        }

        /**
         * Names the messaging bus for command, query, and subscription-query
         * calls. Buses are independent of event store contexts: connections
         * to different contexts share one bus when given the same name.
         * Unset routes messaging to the server's {@code default} bus.
         */
        public Builder busName(String busName) {
            this.busName = busName;
            return this;
        }

        public Builder token(String token) {
            this.token = token;
            return this;
        }

        /** Connects over TLS, trusting the certificates in the PEM file. */
        public Builder useTls(File certFile) throws SSLException {
            this.sslContext = trusting(certFile);
            return this;
        }

        /** Connects over TLS, trusting the JVM's default CAs. */
        public Builder useTls() throws SSLException {
            try {
                this.sslContext = SSLContext.getDefault();
            } catch (GeneralSecurityException e) {
                throw new SSLException(e);
            }
            return this;
        }

        /** Connects over TLS with a context of your own, e.g. one carrying a client certificate. */
        public Builder sslContext(SSLContext sslContext) {
            this.sslContext = Objects.requireNonNull(sslContext);
            return this;
        }

        public Builder connectTimeout(long timeoutMillis) {
            this.connectTimeoutMillis = timeoutMillis;
            return this;
        }

        /**
         * Pings a connection that has been silent for {@code timeMillis}, and
         * closes it if nothing at all arrives within {@code timeoutMillis}
         * after the ping. 0 disables client pings; the server's own pings and
         * the connection's TCP state then decide.
         */
        public Builder keepAlive(long timeMillis, long timeoutMillis) {
            this.keepAliveTimeMillis = timeMillis;
            this.keepAliveTimeoutMillis = timeoutMillis;
            return this;
        }

        /**
         * The largest frame to send or accept. 0 (the default) uses the limit
         * the server announces; a positive value can only lower it.
         */
        public Builder maxInboundMessageSize(int size) {
            this.maxFrame = size;
            return this;
        }

        public KronosDbConnectionFactory build() {
            return new KronosDbConnectionFactory(this);
        }
    }
}
