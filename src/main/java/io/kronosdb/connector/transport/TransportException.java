package io.kronosdb.connector.transport;

/**
 * Base of the errors the native transport raises: a connection that could
 * not be opened or was lost, and requests the server refused.
 */
public class TransportException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public TransportException(String message) {
        super(message);
    }

    public TransportException(String message, Throwable cause) {
        super(message, cause);
    }
}
