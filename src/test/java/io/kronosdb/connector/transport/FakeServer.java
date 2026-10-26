package io.kronosdb.connector.transport;

import io.kronosdb.protocol.Frame;
import io.kronosdb.protocol.Frames;
import io.kronosdb.protocol.Hello;
import io.kronosdb.protocol.Reader;
import io.kronosdb.protocol.Welcome;
import io.kronosdb.protocol.Writer;

import javax.net.ServerSocketFactory;
import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * A scripted native-protocol server for tests: it accepts connections and runs
 * a script on each, speaking the real frames through the generated codec. It
 * only knows what the test tells it, so it checks the client against the
 * schema's contract, not against the real server.
 */
public final class FakeServer implements AutoCloseable {

    /** One accepted connection, as the script sees it. */
    public static final class Session implements AutoCloseable {
        private final Socket socket;
        private final DataInputStream in;
        private final OutputStream out;
        private final Object writeLock = new Object();
        public Hello hello;

        Session(Socket socket) throws IOException {
            this.socket = socket;
            this.in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
            this.out = socket.getOutputStream();
        }

        /** What the client sent after the header, decoded. */
        public record Incoming(int stream, int flags, Frame frame) {
            public boolean end() {
                return (flags & Frames.FLAG_END) != 0;
            }
        }

        /** Reads the preface and the client's Hello. */
        public Hello readHello() throws IOException {
            byte[] preface = in.readNBytes(Frames.PREFACE.length);
            if (!java.util.Arrays.equals(preface, Frames.PREFACE)) {
                throw new IOException("bad preface");
            }
            hello = (Hello) next().frame();
            return hello;
        }

        /** Reads the preface and Hello and answers Welcome. */
        public Hello handshake(long maxFrame, long heartbeatMillis) throws IOException {
            readHello();
            send(0, 0, new Welcome(Frames.PROTOCOL_VERSION, 0, 3, "fake", "tester", maxFrame, heartbeatMillis));
            return hello;
        }

        public Hello handshake() throws IOException {
            return handshake(16 * 1024 * 1024, 10_000);
        }

        /** The next frame from the client; throws EOFException when the client hung up. */
        public Incoming next() throws IOException {
            byte[] header = new byte[Frames.HEADER_LEN];
            in.readFully(header);
            long len = Frames.length(header, 0);
            byte[] body = new byte[(int) len - (Frames.HEADER_LEN - 4)];
            in.readFully(body);
            Frame frame = Frames.decode(Frames.kind(header, 0), new Reader(body));
            if (frame == null) {
                throw new IOException("unknown kind " + Frames.kind(header, 0));
            }
            return new Incoming(Frames.stream(header, 0), Frames.flags(header, 0), frame);
        }

        /** The next frame of the given type, skipping connection frames (pings) the client may interleave. */
        public <T extends Frame> Incoming next(Class<T> type) throws IOException {
            while (true) {
                Incoming incoming = next();
                if (type.isInstance(incoming.frame())) {
                    return incoming;
                }
                if (incoming.stream() != 0) {
                    throw new IOException("expected " + type.getSimpleName() + " but got "
                            + incoming.frame().getClass().getSimpleName());
                }
            }
        }

        public void send(int stream, int flags, Frame frame) throws IOException {
            Writer w = new Writer(256);
            Frames.encode(w, stream, flags, frame);
            synchronized (writeLock) {
                out.write(w.array(), 0, w.position());
                out.flush();
            }
        }

        /** Answers on a stream and ends it. */
        public void reply(int stream, Frame frame) throws IOException {
            send(stream, Frames.FLAG_END, frame);
        }

        public void writeRaw(byte[] bytes) throws IOException {
            synchronized (writeLock) {
                out.write(bytes);
                out.flush();
            }
        }

        @Override
        public void close() {
            try {
                socket.close();
            } catch (IOException ignored) {
                // Already gone.
            }
        }

        public InputStream rawInput() {
            return in;
        }
    }

    private final ServerSocket serverSocket;
    private final Consumer<Session> script;
    private final AtomicInteger connections = new AtomicInteger();
    private final List<Session> sessions = new CopyOnWriteArrayList<>();
    private volatile boolean closed;

    public FakeServer(Consumer<Session> script) throws IOException {
        this(ServerSocketFactory.getDefault().createServerSocket(0, 50, InetAddress.getLoopbackAddress()), script);
    }

    public FakeServer(ServerSocket serverSocket, Consumer<Session> script) {
        this.serverSocket = serverSocket;
        this.script = script;
        Thread acceptor = new Thread(this::acceptLoop, "fake-server-acceptor");
        acceptor.setDaemon(true);
        acceptor.start();
    }

    public int port() {
        return serverSocket.getLocalPort();
    }

    public int connections() {
        return connections.get();
    }

    public List<Session> sessions() {
        return sessions;
    }

    private void acceptLoop() {
        while (!closed) {
            try {
                Socket socket = serverSocket.accept();
                connections.incrementAndGet();
                Thread t = new Thread(() -> run(socket), "fake-server-connection");
                t.setDaemon(true);
                t.start();
            } catch (IOException e) {
                return;
            }
        }
    }

    private void run(Socket socket) {
        try (Session session = new Session(socket)) {
            sessions.add(session);
            script.accept(session);
        } catch (IOException e) {
            // The client hung up or the test closed the socket.
        } catch (RuntimeException e) {
            if (!(e.getCause() instanceof EOFException)) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void close() {
        closed = true;
        try {
            serverSocket.close();
        } catch (IOException ignored) {
            // Nothing to release.
        }
        sessions.forEach(Session::close);
    }
}
