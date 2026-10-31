package io.kronosdb.connector.transport;

/**
 * One stream's window: the bytes of frames the server may send on it ahead of
 * the client. The server counts every frame it sends, except the one that ends
 * the stream, and waits while the window is used up. The client grants the
 * bytes back as its consumer takes frames, accumulating them and sending one
 * {@code Window} frame once half the window has been consumed, so the server
 * keeps sending while the grant travels.
 *
 * <p>Granting never blocks: a {@code Window} frame is queued like any other
 * and may follow the client's own end of the stream. Once the stream has
 * finished at the client there is nothing to grant to, and nothing is sent.
 */
public final class StreamWindow {

    private final NativeConnection connection;
    private final int stream;
    private final long threshold;

    private long consumed;

    StreamWindow(NativeConnection connection, int stream, int window) {
        this.connection = connection;
        this.stream = stream;
        this.threshold = window / 2;
    }

    /** The consumer took a frame of {@code bytes} bytes (its size on the wire) from the stream. */
    public void consumed(int bytes) {
        long grant;
        synchronized (this) {
            consumed += bytes;
            if (consumed < threshold) {
                return;
            }
            grant = consumed;
            consumed = 0;
        }
        connection.grant(stream, grant);
    }
}
