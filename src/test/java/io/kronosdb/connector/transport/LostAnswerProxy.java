package io.kronosdb.connector.transport;

import io.kronosdb.protocol.Appended;
import io.kronosdb.protocol.ErrorCode;
import io.kronosdb.protocol.Frames;
import io.kronosdb.protocol.Reader;
import io.kronosdb.protocol.RequestFailed;
import io.kronosdb.protocol.Writer;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A TCP proxy in front of a real server that loses the answer to one append.
 * Everything passes through untouched, except the first {@code Appended} the
 * server sends: the client never sees it. The server wrote that append, so the
 * client is where the protocol says it may be: not knowing.
 */
public final class LostAnswerProxy implements AutoCloseable {

    /** What the client gets in place of the first {@code Appended}. */
    public enum Instead {
        /** The connection closes. */
        A_CLOSED_CONNECTION,
        /** {@code RequestFailed} with {@code unavailable}, as when the append was not confirmed in time. */
        UNAVAILABLE
    }

    private final String host;
    private final int port;
    private final Instead instead;
    private final ServerSocket listener;
    private final AtomicBoolean lost = new AtomicBoolean();
    private final AtomicInteger answers = new AtomicInteger();
    private final List<Socket> sockets = new CopyOnWriteArrayList<>();

    public LostAnswerProxy(String host, int port, Instead instead) throws IOException {
        this.host = host;
        this.port = port;
        this.instead = instead;
        this.listener = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
        Thread acceptor = new Thread(this::accept, "lost-answer-proxy");
        acceptor.setDaemon(true);
        acceptor.start();
    }

    public int port() {
        return listener.getLocalPort();
    }

    /** How many {@code Appended} frames the server has sent, the lost one included. */
    public int appendedByTheServer() {
        return answers.get();
    }

    private void accept() {
        while (true) {
            try {
                Socket client = listener.accept();
                Socket server = new Socket(host, port);
                client.setTcpNoDelay(true);
                server.setTcpNoDelay(true);
                sockets.add(client);
                sockets.add(server);
                Thread.startVirtualThread(() -> toServer(client, server));
                Thread.startVirtualThread(() -> toClient(server, client));
            } catch (IOException e) {
                return;
            }
        }
    }

    private void toServer(Socket client, Socket server) {
        try {
            client.getInputStream().transferTo(server.getOutputStream());
        } catch (IOException ignored) {
            // One side hung up.
        } finally {
            closeBoth(client, server);
        }
    }

    private void toClient(Socket server, Socket client) {
        try {
            DataInputStream in = new DataInputStream(server.getInputStream());
            OutputStream out = client.getOutputStream();
            byte[] header = new byte[Frames.HEADER_LEN];
            while (true) {
                in.readFully(header);
                byte[] body = new byte[(int) Frames.length(header, 0) - (Frames.HEADER_LEN - 4)];
                in.readFully(body);
                boolean appended = Frames.decode(Frames.kind(header, 0), new Reader(body)) instanceof Appended;
                if (appended) {
                    answers.incrementAndGet();
                }
                if (appended && lost.compareAndSet(false, true)) {
                    if (instead == Instead.A_CLOSED_CONNECTION) {
                        return;
                    }
                    Writer failed = new Writer(64);
                    Frames.encode(failed, Frames.stream(header, 0), Frames.FLAG_END,
                            new RequestFailed(ErrorCode.UNAVAILABLE, "the append was not confirmed in time"));
                    out.write(failed.array(), 0, failed.position());
                } else {
                    out.write(header);
                    out.write(body);
                }
                out.flush();
            }
        } catch (IOException ignored) {
            // One side hung up.
        } finally {
            closeBoth(client, server);
        }
    }

    private static void closeBoth(Socket a, Socket b) {
        for (Socket socket : List.of(a, b)) {
            try {
                socket.close();
            } catch (IOException ignored) {
                // Already gone.
            }
        }
    }

    @Override
    public void close() throws IOException {
        listener.close();
        for (Socket socket : sockets) {
            socket.close();
        }
    }
}
