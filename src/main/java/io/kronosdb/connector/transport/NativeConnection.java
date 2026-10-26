package io.kronosdb.connector.transport;

import io.kronosdb.protocol.Cancel;
import io.kronosdb.protocol.BindBus;
import io.kronosdb.protocol.BindContext;
import io.kronosdb.protocol.BusBound;
import io.kronosdb.protocol.ClientPong;
import io.kronosdb.protocol.ContextBound;
import io.kronosdb.protocol.CredentialKind;
import io.kronosdb.protocol.Frame;
import io.kronosdb.protocol.Frames;
import io.kronosdb.protocol.GoAway;
import io.kronosdb.protocol.Hello;
import io.kronosdb.protocol.Ping;
import io.kronosdb.protocol.Pong;
import io.kronosdb.protocol.ProtocolException;
import io.kronosdb.protocol.Reader;
import io.kronosdb.protocol.RequestFailed;
import io.kronosdb.protocol.ServerPing;
import io.kronosdb.protocol.Welcome;
import io.kronosdb.protocol.Writer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;
import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * One TCP connection to a KronosDB server speaking the native protocol, with
 * any number of requests and streams in flight on it.
 *
 * <h2>Threads</h2>
 * One daemon thread reads the socket and routes each frame to the stream
 * it belongs to; platform threads read with the least wake-up latency. There
 * is no writer thread: a caller encodes its frame into a shared buffer under
 * a lock and, if nobody is writing, becomes the writer and sends everything
 * queued so far with one {@code write}. Frames that other callers queue
 * meanwhile go out in the same write, so concurrent requests coalesce without
 * a hand-off to a writer thread at concurrency 1. A writer that keeps finding
 * more work after a few rounds hands the rest to a virtual thread, so one
 * caller is never stuck sending for everyone else.
 *
 * <h2>Memory</h2>
 * Each received frame is read into its own array, and decoded bytes (event
 * payloads, tags) are slices of it; nothing is copied until the caller does.
 * Each sent frame is encoded straight into the shared output buffer.
 */
public final class NativeConnection implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(NativeConnection.class);

    /** The stored record layout this client decodes. */
    private static final int RECORD_FORMAT = 3;
    /** Bound for frames before Welcome announces the real limit. */
    private static final int HANDSHAKE_MAX_FRAME = 16 * 1024 * 1024;
    /** Producers wait while this much is queued unsent. */
    private static final int HIGH_WATER = 8 * 1024 * 1024;
    /** Write rounds one caller does before handing the remaining work to a virtual thread. */
    private static final int FLUSH_ROUNDS = 4;
    private static final int BUFFER_SIZE = 64 * 1024;
    private static final int BUFFER_KEPT = 1024 * 1024;

    private final ConnectOptions options;
    private final String peer;
    private final Socket socket;
    private final InputStream in;
    private final OutputStream out;
    private final Welcome welcome;
    private final int maxFrame;
    private final long idleTimeoutMillis;
    private final boolean clientPings;

    private final ConcurrentHashMap<Integer, StreamListener> streams = new ConcurrentHashMap<>();
    private final AtomicInteger lastStreamId = new AtomicInteger();

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition drained = lock.newCondition();
    private Writer pending = new Writer(BUFFER_SIZE);
    private Writer idle = new Writer(BUFFER_SIZE);
    private boolean flushing;

    private final AtomicBoolean closed = new AtomicBoolean();
    private volatile Throwable closeCause;
    private volatile boolean draining;

    // Only the reader thread touches these.
    private boolean pingOutstanding;
    private long pingNonce;

    private final ConcurrentHashMap<String, CompletableFuture<Integer>> contexts = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, CompletableFuture<Integer>> buses = new ConcurrentHashMap<>();
    private final AtomicInteger nextContextId = new AtomicInteger(1);
    private final AtomicInteger nextBusId = new AtomicInteger(1);

    private NativeConnection(ConnectOptions options, Socket socket, InputStream in, OutputStream out,
                             Welcome welcome) {
        this.options = options;
        this.peer = options.host() + ":" + options.port();
        this.socket = socket;
        this.in = in;
        this.out = out;
        this.welcome = welcome;
        long announced = Math.min(welcome.maxFrame(), Integer.MAX_VALUE);
        this.maxFrame = options.maxFrame() > 0 ? (int) Math.min(options.maxFrame(), announced) : (int) announced;
        this.clientPings = options.keepAliveMillis() > 0;
        this.idleTimeoutMillis = clientPings
                ? options.keepAliveMillis()
                : Math.max(3 * welcome.heartbeat(), 1000);
        contexts.put("default", CompletableFuture.completedFuture(0));
        buses.put("default", CompletableFuture.completedFuture(0));
    }

    /** Opens a connection and performs the handshake. */
    public static NativeConnection open(ConnectOptions options) {
        String peer = options.host() + ":" + options.port();
        Socket socket = new Socket();
        try {
            socket.setTcpNoDelay(true);
            socket.setKeepAlive(true);
            int connectTimeout = (int) Math.max(1, options.connectTimeoutMillis());
            socket.connect(new InetSocketAddress(options.host(), options.port()), connectTimeout);
            Socket io = socket;
            if (options.sslContext() != null) {
                SSLSocket tls = (SSLSocket) options.sslContext().getSocketFactory()
                        .createSocket(socket, options.host(), options.port(), true);
                SSLParameters parameters = tls.getSSLParameters();
                parameters.setEndpointIdentificationAlgorithm("HTTPS");
                tls.setSSLParameters(parameters);
                tls.setSoTimeout(connectTimeout);
                tls.startHandshake();
                io = tls;
            }
            io.setSoTimeout(connectTimeout);
            OutputStream out = io.getOutputStream();
            InputStream in = new BufferedInputStream(io.getInputStream(), BUFFER_SIZE);

            String token = options.token();
            boolean hasToken = token != null && !token.isEmpty();
            Writer hello = new Writer(256);
            hello.writeRaw(Frames.PREFACE, 0, Frames.PREFACE.length);
            Frames.encode(hello, 0, 0, new Hello(
                    Frames.PROTOCOL_VERSION,
                    0,
                    RECORD_FORMAT,
                    options.clientName(),
                    options.clientId(),
                    hasToken ? CredentialKind.TOKEN : CredentialKind.NONE,
                    hasToken ? token : ""));
            out.write(hello.array(), 0, hello.position());
            out.flush();

            Frame first = readHandshakeFrame(in);
            if (first instanceof RequestFailed failed) {
                throw new RequestFailedException(failed.code(), failed.message());
            }
            if (!(first instanceof Welcome welcome)) {
                throw new ConnectionClosedException(
                        "the server at " + peer + " answered the handshake with " + first.getClass().getSimpleName()
                                + " instead of Welcome");
            }
            NativeConnection connection = new NativeConnection(options, io, in, out, welcome);
            io.setSoTimeout((int) Math.min(connection.idleTimeoutMillis, Integer.MAX_VALUE));
            Thread reader = new Thread(connection::readLoop, "kronosdb-reader-" + peer);
            reader.setDaemon(true);
            reader.start();
            logger.debug("Connected to KronosDB [{}] as [{}], protocol version {}.", welcome.serverName(),
                    welcome.principal(), welcome.version());
            return connection;
        } catch (IOException | ProtocolException e) {
            closeQuietly(socket);
            throw new ConnectionClosedException("could not connect to KronosDB at " + peer + ": " + e.getMessage(), e);
        } catch (RuntimeException e) {
            closeQuietly(socket);
            throw e;
        }
    }

    private static Frame readHandshakeFrame(InputStream in) throws IOException {
        DataInputStream data = new DataInputStream(in);
        byte[] header = new byte[Frames.HEADER_LEN];
        data.readFully(header);
        long len = Frames.length(header, 0);
        if (len + 4 > HANDSHAKE_MAX_FRAME) {
            throw new ProtocolException("handshake frame of " + (len + 4) + " bytes");
        }
        byte[] body = new byte[(int) len - (Frames.HEADER_LEN - 4)];
        data.readFully(body);
        Frame frame = Frames.decode(Frames.kind(header, 0), new Reader(body));
        if (frame == null) {
            throw new ProtocolException("unknown frame kind " + Frames.kind(header, 0) + " in the handshake");
        }
        return frame;
    }

    // ------------------------------------------------------------------ state

    public Welcome welcome() {
        return welcome;
    }

    public boolean isClosed() {
        return closed.get();
    }

    /** Whether new requests should use this connection: open, and not told to go away. */
    public boolean isUsable() {
        return !closed.get() && !draining;
    }

    @Override
    public void close() {
        fail(new ConnectionClosedException("the connection was closed by the client"), false);
    }

    private ConnectionClosedException closedException() {
        Throwable cause = closeCause;
        if (cause instanceof ConnectionClosedException c) {
            return new ConnectionClosedException(c.getMessage(), c);
        }
        return new ConnectionClosedException("the connection to " + peer + " is closed", cause);
    }

    private void fail(ConnectionClosedException cause) {
        fail(cause, true);
    }

    private void fail(ConnectionClosedException cause, boolean log) {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        closeCause = cause;
        if (log) {
            logger.info("Connection to KronosDB at [{}] closed: {}", peer, cause.getMessage());
        }
        closeQuietly(socket);
        lock.lock();
        try {
            pending.reset();
            drained.signalAll();
        } finally {
            lock.unlock();
        }
        for (Integer id : streams.keySet()) {
            StreamListener listener = streams.remove(id);
            if (listener != null) {
                notifyFailure(listener, cause);
            }
        }
    }

    private static void closeQuietly(Socket socket) {
        try {
            socket.close();
        } catch (IOException ignored) {
            // Nothing left to release.
        }
    }

    private static void notifyFailure(StreamListener listener, Throwable cause) {
        try {
            listener.onFailure(cause);
        } catch (RuntimeException e) {
            logger.warn("A stream listener failed while handling a connection failure.", e);
        }
    }

    // --------------------------------------------------------------- requests

    /** Sends a request that is answered by exactly one frame. */
    public CompletableFuture<Frame> request(Frame request) {
        Unary reply = new Unary();
        int id;
        try {
            id = register(reply);
        } catch (RuntimeException e) {
            return CompletableFuture.failedFuture(e);
        }
        try {
            send(id, Frames.FLAG_END, request, true);
        } catch (RuntimeException e) {
            streams.remove(id, reply);
            reply.completeExceptionally(e);
        }
        return reply;
    }

    /**
     * Opens a stream with its first frame. With {@code end} the client sends
     * nothing more and only reads (a Source); without it the client may keep
     * sending on the returned stream (credits, a handler's answers).
     */
    public ClientStream open(Frame first, boolean end, StreamListener listener) {
        int id = register(listener);
        try {
            send(id, end ? Frames.FLAG_END : 0, first, true);
        } catch (RuntimeException e) {
            streams.remove(id, listener);
            throw e;
        }
        return new ClientStream(this, id);
    }

    /**
     * The id of the context named {@code name} on this connection, binding it
     * with the server on first use. The default context is bound from the start.
     */
    public CompletableFuture<Integer> context(String name) {
        return bind(contexts, nextContextId, name, false);
    }

    /** The id of the messaging bus named {@code name}; the default bus is bound from the start. */
    public CompletableFuture<Integer> bus(String name) {
        return bind(buses, nextBusId, name, true);
    }

    private CompletableFuture<Integer> bind(ConcurrentHashMap<String, CompletableFuture<Integer>> bound,
                                            AtomicInteger counter, String name, boolean bus) {
        CompletableFuture<Integer> existing = bound.get(name);
        if (existing != null) {
            return existing;
        }
        CompletableFuture<Integer> mine = new CompletableFuture<>();
        existing = bound.putIfAbsent(name, mine);
        if (existing != null) {
            return existing;
        }
        int id = counter.getAndIncrement();
        if (id > 0xFFFF) {
            bound.remove(name, mine);
            mine.completeExceptionally(new TransportException(
                    "more than 65536 " + (bus ? "buses" : "contexts") + " on one connection"));
            return mine;
        }
        Frame request = bus ? new BindBus(id, name) : new BindContext(id, name);
        request(request).whenComplete((reply, error) -> {
            if (error != null) {
                bound.remove(name, mine);
                mine.completeExceptionally(Failures.unwrap(error));
            } else if (reply instanceof ContextBound || reply instanceof BusBound) {
                mine.complete(id);
            } else {
                bound.remove(name, mine);
                mine.completeExceptionally(new TransportException(
                        "unexpected " + reply.getClass().getSimpleName() + " when binding " + name));
            }
        });
        return mine;
    }

    // --------------------------------------------------------------- streams

    private int register(StreamListener listener) {
        if (closed.get()) {
            throw closedException();
        }
        int id;
        do {
            id = lastStreamId.incrementAndGet();
            // Ids wrap past 2^32 and skip 0, which is the connection itself.
        } while (id == 0 || streams.putIfAbsent(id, listener) != null);
        if (closed.get()) {
            streams.remove(id, listener);
            throw closedException();
        }
        return id;
    }

    boolean release(int id) {
        return streams.remove(id) != null;
    }

    boolean isOpen(int id) {
        return streams.containsKey(id);
    }

    // --------------------------------------------------------------- writing

    void send(int stream, int flags, Frame frame, boolean mayBlock) {
        boolean lead;
        lock.lock();
        try {
            if (mayBlock) {
                while (pending.position() >= HIGH_WATER && !closed.get()) {
                    drained.awaitUninterruptibly();
                }
            }
            if (closed.get()) {
                throw closedException();
            }
            int n = Frames.encode(pending, stream, flags, frame);
            if (n > maxFrame) {
                pending.truncate(pending.position() - n);
                throw new TransportException("a " + frame.getClass().getSimpleName() + " frame of " + n
                        + " bytes exceeds the maximum frame size of " + maxFrame + " bytes");
            }
            lead = !flushing;
            if (lead) {
                flushing = true;
            }
        } finally {
            lock.unlock();
        }
        if (lead) {
            if (mayBlock) {
                flush();
            } else {
                // The reader thread must never wait on the socket it also drains.
                Thread.startVirtualThread(this::flush);
            }
        }
    }

    private void flush() {
        int rounds = 0;
        while (true) {
            Writer batch;
            lock.lock();
            try {
                if (pending.position() == 0 || closed.get()) {
                    flushing = false;
                    return;
                }
                if (rounds == FLUSH_ROUNDS) {
                    // Still the writer (flushing stays set); someone else's turn to spend the time.
                    Thread.startVirtualThread(this::flush);
                    return;
                }
                batch = pending;
                pending = idle;
                idle = batch;
                drained.signalAll();
            } finally {
                lock.unlock();
            }
            try {
                out.write(batch.array(), 0, batch.position());
                out.flush();
            } catch (IOException e) {
                lock.lock();
                try {
                    flushing = false;
                } finally {
                    lock.unlock();
                }
                fail(new ConnectionClosedException("writing to " + peer + " failed: " + e.getMessage(), e));
                return;
            }
            batch.reset();
            if (batch.capacity() > BUFFER_KEPT) {
                batch.shrinkTo(BUFFER_SIZE);
            }
            rounds++;
        }
    }

    // --------------------------------------------------------------- reading

    private void readLoop() {
        try {
            byte[] header = new byte[Frames.HEADER_LEN];
            while (true) {
                if (!readFully(header, 0, header.length, true)) {
                    fail(new ConnectionClosedException("the server at " + peer + " closed the connection"));
                    return;
                }
                long len = Frames.length(header, 0);
                if (len + 4 > maxFrame) {
                    throw new ProtocolException("a frame of " + (len + 4) + " bytes exceeds the limit of "
                            + maxFrame);
                }
                byte[] body = new byte[(int) len - (Frames.HEADER_LEN - 4)];
                readFully(body, 0, body.length, false);
                dispatch(Frames.kind(header, 0), Frames.flags(header, 0), Frames.stream(header, 0), body);
            }
        } catch (IOException e) {
            fail(new ConnectionClosedException("connection to " + peer + " lost: " + e.getMessage(), e));
        } catch (ProtocolException e) {
            fail(new ConnectionClosedException("protocol violation by " + peer + ": " + e.getMessage(), e));
        } catch (RuntimeException | Error e) {
            logger.error("The KronosDB reader for [{}] failed.", peer, e);
            fail(new ConnectionClosedException("reader for " + peer + " failed: " + e, e));
        }
    }

    /** Reads exactly {@code len} bytes; false on a clean end of stream before the first byte of a frame. */
    private boolean readFully(byte[] buf, int off, int len, boolean atFrameBoundary) throws IOException {
        int done = 0;
        while (done < len) {
            int n;
            try {
                n = in.read(buf, off + done, len - done);
            } catch (SocketTimeoutException e) {
                if (!onSilence()) {
                    throw new IOException("no sign of life from the server for "
                            + (idleTimeoutMillis + options.keepAliveTimeoutMillis()) + " ms");
                }
                continue;
            }
            if (n < 0) {
                if (done == 0 && atFrameBoundary) {
                    return false;
                }
                throw new EOFException("the server closed the connection in the middle of a frame");
            }
            done += n;
            if (pingOutstanding) {
                pingOutstanding = false;
                socket.setSoTimeout((int) Math.min(idleTimeoutMillis, Integer.MAX_VALUE));
            }
        }
        return true;
    }

    /** The connection was silent for the idle timeout. True to keep waiting. */
    private boolean onSilence() throws IOException {
        if (!clientPings || pingOutstanding) {
            return false;
        }
        pingOutstanding = true;
        socket.setSoTimeout((int) Math.min(Math.max(options.keepAliveTimeoutMillis(), 1), Integer.MAX_VALUE));
        try {
            send(0, 0, new Ping(++pingNonce), false);
        } catch (RuntimeException e) {
            return false;
        }
        return true;
    }

    private void dispatch(int kind, int flags, int stream, byte[] body) {
        if ((kind & 0x8000) == 0) {
            throw new ProtocolException("the server sent a client frame, kind " + kind);
        }
        if (stream == 0) {
            control(kind, body);
            return;
        }
        StreamListener listener = streams.get(stream);
        if (listener == null) {
            // The request finished or was cancelled; late frames are dropped.
            return;
        }
        Frame frame = Frames.decode(kind, new Reader(body));
        if (frame == null) {
            streams.remove(stream);
            notifyFailure(listener, new TransportException("the server sent a frame of unknown kind " + kind));
            return;
        }
        if (frame instanceof RequestFailed failed) {
            streams.remove(stream);
            notifyFailure(listener, new RequestFailedException(failed.code(), failed.message()));
            return;
        }
        boolean last = (flags & Frames.FLAG_END) != 0 || listener instanceof Unary;
        if (last) {
            streams.remove(stream);
        }
        try {
            listener.onFrame(frame, last);
        } catch (RuntimeException e) {
            logger.warn("A stream listener failed while handling {}.", frame.getClass().getSimpleName(), e);
        }
    }

    private void control(int kind, byte[] body) {
        Frame frame = Frames.decode(kind, new Reader(body));
        switch (frame) {
            case ServerPing ping -> {
                try {
                    send(0, 0, new ClientPong(ping.nonce()), false);
                } catch (RuntimeException e) {
                    // Closed meanwhile; the reader notices on its next read.
                }
            }
            case Pong ignored -> {
            }
            case GoAway goAway -> {
                draining = true;
                logger.info("KronosDB at [{}] is going away: {}", peer, goAway.message());
            }
            case RequestFailed failed -> throw new ProtocolException(
                    "the server closed the connection: " + failed.code() + ": " + failed.message());
            case null, default -> throw new ProtocolException("only connection frames travel on stream 0");
        }
    }

    /** A request answered by one frame. */
    private static final class Unary extends CompletableFuture<Frame> implements StreamListener {
        @Override
        public void onFrame(Frame frame, boolean last) {
            complete(frame);
        }

        @Override
        public void onFailure(Throwable cause) {
            completeExceptionally(cause);
        }
    }

    /** Cancels a stream the client opened. */
    void cancel(int id) {
        if (streams.remove(id) != null && !closed.get()) {
            try {
                send(id, Frames.FLAG_END, new Cancel(), true);
            } catch (RuntimeException e) {
                // Best effort: the connection is in trouble anyway.
            }
        }
    }
}
