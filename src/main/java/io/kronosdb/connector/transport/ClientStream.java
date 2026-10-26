package io.kronosdb.connector.transport;

import io.kronosdb.protocol.Frame;
import io.kronosdb.protocol.Frames;

/**
 * The sending half of a stream the client opened with
 * {@link NativeConnection#open}: more frames of the client's (credits, a
 * handler's answers) and cancellation. The server's frames go to the
 * {@link StreamListener} the stream was opened with.
 */
public final class ClientStream {

    private final NativeConnection connection;
    private final int id;

    ClientStream(NativeConnection connection, int id) {
        this.connection = connection;
        this.id = id;
    }

    /** Sends a frame on the stream. Throws {@link ConnectionClosedException} if the connection is gone. */
    public void send(Frame frame) {
        connection.send(id, 0, frame, true);
    }

    /** Sends a last frame: the client sends nothing more on the stream. */
    public void sendEnd(Frame frame) {
        connection.send(id, Frames.FLAG_END, frame, true);
    }

    /** Whether the stream is still open at the client: not finished, failed or cancelled. */
    public boolean isOpen() {
        return connection.isOpen(id);
    }

    /**
     * Ends the stream: late frames from the server are dropped, and the server
     * is told to stop. Does nothing if the stream already finished.
     */
    public void cancel() {
        connection.cancel(id);
    }
}
