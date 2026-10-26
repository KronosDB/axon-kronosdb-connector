package io.kronosdb.connector.transport;

/**
 * The connection to the server is closed or could not be opened. Requests
 * that were in flight when it closed fail with this; the next request opens
 * a new connection.
 */
public class ConnectionClosedException extends TransportException {

    private static final long serialVersionUID = 1L;

    public ConnectionClosedException(String message) {
        super(message);
    }

    public ConnectionClosedException(String message, Throwable cause) {
        super(message, cause);
    }
}
