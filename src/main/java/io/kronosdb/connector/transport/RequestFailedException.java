package io.kronosdb.connector.transport;

import io.kronosdb.protocol.ErrorCode;

/**
 * The server answered a request with {@code RequestFailed}. The message is
 * the server's: it says what happened and, where there is something to do,
 * what to do.
 */
public class RequestFailedException extends TransportException {

    private static final long serialVersionUID = 1L;

    private final transient ErrorCode code;

    public RequestFailedException(ErrorCode code, String message) {
        super(message);
        this.code = code;
    }

    /** Why the request failed. A code this version does not know should be treated as {@code INTERNAL}. */
    public ErrorCode code() {
        return code;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + " [" + code + "]: " + getMessage();
    }
}
