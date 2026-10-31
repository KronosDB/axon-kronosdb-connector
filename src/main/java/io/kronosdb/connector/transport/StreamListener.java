package io.kronosdb.connector.transport;

import io.kronosdb.protocol.Frame;

/**
 * Receives the frames the server sends on one stream. Called from the
 * connection's reader thread, one frame at a time, so it must not block: hand
 * work that can to another thread.
 *
 * <p>A frame counts as taken by its consumer when {@code onFrame} returns, and
 * the connection grants its bytes back to the server's window for the stream
 * then. A listener that queues frames for a consumer that reads at its own
 * pace implements {@link Paced} instead, so the window follows the consumer.
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

    /**
     * A listener that keeps frames for a consumer and tells the stream's
     * window when the consumer has taken each one. Until it does, the server
     * may send no more than a window ahead on the stream, so a consumer that
     * falls behind holds back its own stream and nothing else.
     */
    interface Paced extends StreamListener {

        /**
         * A frame for the stream.
         *
         * @param last   whether the server ended the stream with this frame
         * @param size   the frame's size on the wire, which the window counts; 0 for the frame
         *               that ends the stream, which counts for nothing
         * @param window the stream's window: call {@link StreamWindow#consumed} with {@code size}
         *               once the consumer has taken the frame, or right away for a frame
         *               nobody will read
         */
        void onFrame(Frame frame, boolean last, int size, StreamWindow window);

        @Override
        default void onFrame(Frame frame, boolean last) {
            throw new UnsupportedOperationException("a paced listener is called with the size of the frame");
        }
    }
}
