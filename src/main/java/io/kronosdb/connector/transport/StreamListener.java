package io.kronosdb.connector.transport;

import io.kronosdb.protocol.Frame;

/**
 * Receives the frames the server sends on one stream. Called from the
 * connection's reader thread, one frame at a time, so it must not block: hand
 * work that can to another thread.
 */
public interface StreamListener {

    /**
     * A frame for the stream.
     *
     * @param last whether the server ended the stream with this frame
     */
    void onFrame(Frame frame, boolean last);

    /**
     * The stream is over without a normal end: the server answered with
     * {@code RequestFailed} ({@link RequestFailedException}), or the
     * connection was lost ({@link ConnectionClosedException}).
     */
    void onFailure(Throwable cause);
}
