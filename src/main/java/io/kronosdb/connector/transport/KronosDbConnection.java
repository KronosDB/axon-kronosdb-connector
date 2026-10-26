package io.kronosdb.connector.transport;

import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * A handle on a KronosDB server for a specific context. Provides access to
 * typed channel abstractions for event store, command bus, query bus, and
 * platform operations. The channels share the factory's one native
 * connection; the context is named on it by a small id the connection binds.
 */
public class KronosDbConnection {

    private static final Logger logger = LoggerFactory.getLogger(KronosDbConnection.class);

    private final Transport transport;
    private final String context;
    private final @Nullable String busName;

    private volatile boolean disconnected;
    private volatile @Nullable EventStoreChannel eventStoreChannel;
    private volatile @Nullable CommandChannel commandChannel;
    private volatile @Nullable QueryChannel queryChannel;
    private volatile @Nullable PlatformChannel platformChannel;

    KronosDbConnection(Transport transport, String context, @Nullable String busName) {
        this.transport = Objects.requireNonNull(transport, "The Transport must not be null.");
        this.context = Objects.requireNonNull(context, "The context must not be null.");
        this.busName = busName;
    }

    /**
     * Returns the event store channel for this connection.
     */
    public EventStoreChannel eventStoreChannel() {
        if (eventStoreChannel == null) {
            synchronized (this) {
                if (eventStoreChannel == null) {
                    eventStoreChannel = new EventStoreChannel(transport, context);
                }
            }
        }
        return eventStoreChannel;
    }

    /**
     * Returns the command channel for this connection.
     */
    public CommandChannel commandChannel() {
        if (commandChannel == null) {
            synchronized (this) {
                if (commandChannel == null) {
                    commandChannel = new CommandChannel(transport, context, busName);
                }
            }
        }
        return commandChannel;
    }

    /**
     * Returns the query channel for this connection.
     */
    public QueryChannel queryChannel() {
        if (queryChannel == null) {
            synchronized (this) {
                if (queryChannel == null) {
                    queryChannel = new QueryChannel(transport, context, busName);
                }
            }
        }
        return queryChannel;
    }

    /**
     * Returns the platform channel for this connection.
     */
    public PlatformChannel platformChannel() {
        if (platformChannel == null) {
            synchronized (this) {
                if (platformChannel == null) {
                    platformChannel = new PlatformChannel(transport, context);
                }
            }
        }
        return platformChannel;
    }

    /**
     * Returns the context this connection is associated with.
     */
    public String context() {
        return context;
    }

    /**
     * Returns whether this connection has not been disconnected. The server
     * connection underneath is opened on first use and replaced if it is lost.
     */
    public boolean isConnected() {
        return !disconnected;
    }

    /**
     * Disconnects this connection: its streams end, and the handlers it
     * registered are unregistered. The server connection is shared and closes
     * with the factory.
     */
    public void disconnect() {
        if (disconnected) {
            return;
        }
        disconnected = true;
        logger.info("Disconnecting KronosDB connection for context [{}].", context);
        PlatformChannel platform = platformChannel;
        if (platform != null) {
            platform.disconnect();
        }
        CommandChannel commands = commandChannel;
        if (commands != null) {
            commands.prepareDisconnect();
        }
        QueryChannel queries = queryChannel;
        if (queries != null) {
            queries.prepareDisconnect();
        }
    }
}
