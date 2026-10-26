package io.kronosdb.connector.transport;

import io.kronosdb.protocol.GetHead;
import io.kronosdb.protocol.Head;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLServerSocket;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * TLS: the connection is encrypted, trusts the certificate file it is given,
 * and checks the server's name. Certificates are made with the JDK's keytool.
 */
class TlsConnectionTest {

    private static final String PASSWORD = "changeit";
    private static Path dir;
    private static Path matching;
    private static Path other;

    @BeforeAll
    static void certificates() throws Exception {
        dir = Files.createDirectories(Path.of("target", "tls-test"));
        matching = certificate("matching", "dns:localhost,ip:127.0.0.1");
        other = certificate("other", "dns:somewhere-else.example");
    }

    private static Path certificate(String name, String san) throws Exception {
        Path keystore = dir.resolve(name + ".p12");
        Files.deleteIfExists(keystore);
        run("keytool", "-genkeypair", "-alias", name, "-keyalg", "RSA", "-keysize", "2048",
                "-dname", "CN=" + name, "-ext", "san=" + san, "-validity", "2",
                "-storetype", "PKCS12", "-keystore", keystore.toString(),
                "-storepass", PASSWORD, "-keypass", PASSWORD);
        Path pem = dir.resolve(name + ".pem");
        Files.deleteIfExists(pem);
        run("keytool", "-exportcert", "-rfc", "-alias", name, "-keystore", keystore.toString(),
                "-storepass", PASSWORD, "-file", pem.toString());
        return keystore;
    }

    private static void run(String... command) throws Exception {
        Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
        String output = new String(process.getInputStream().readAllBytes());
        assertThat(process.waitFor(30, TimeUnit.SECONDS)).isTrue();
        assertThat(process.exitValue()).as(output).isZero();
    }

    private static FakeServer tlsServer(Path keystore) throws Exception {
        KeyStore store = KeyStore.getInstance("PKCS12");
        try (FileInputStream in = new FileInputStream(keystore.toFile())) {
            store.load(in, PASSWORD.toCharArray());
        }
        KeyManagerFactory keys = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        keys.init(store, PASSWORD.toCharArray());
        SSLContext context = SSLContext.getInstance("TLS");
        context.init(keys.getKeyManagers(), null, null);
        SSLServerSocket socket = (SSLServerSocket) context.getServerSocketFactory()
                .createServerSocket(0, 50, InetAddress.getLoopbackAddress());
        return new FakeServer(socket, session -> {
            try {
                session.handshake();
                while (true) {
                    FakeServer.Session.Incoming in = session.next(GetHead.class);
                    session.reply(in.stream(), new Head(7));
                }
            } catch (IOException ignored) {
                // The client hung up, or never finished the handshake.
            }
        });
    }

    private static ConnectOptions options(FakeServer server, String host, File trust) throws Exception {
        return new ConnectOptions(host, server.port(), "t", "c", null,
                trust == null ? javax.net.ssl.SSLContext.getDefault() : KronosDbConnectionFactory.trusting(trust),
                5000, 0, 0, 0);
    }

    @Test
    void aConnectionOverTlsTrustsTheCertificateFileAndWorks() throws Exception {
        try (FakeServer server = tlsServer(matching);
             Transport transport = new Transport(options(server, "localhost", dir.resolve("matching.pem").toFile()))) {
            assertThat(transport.connection().request(new GetHead(0)).get(10, TimeUnit.SECONDS))
                    .isEqualTo(new Head(7));
        }
    }

    @Test
    void theFactoryUsesTheSameWayToTrust() throws Exception {
        try (FakeServer server = tlsServer(matching)) {
            KronosDbConnectionFactory factory = KronosDbConnectionFactory.forClient("t", "c")
                    .host("localhost").port(server.port()).useTls(dir.resolve("matching.pem").toFile()).build();
            try {
                assertThat(factory.connect("default").eventStoreChannel().head().get(10, TimeUnit.SECONDS))
                        .isEqualTo(7);
            } finally {
                factory.shutdown();
            }
        }
    }

    @Test
    void aCertificateIssuedForAnotherNameIsRefused() throws Exception {
        try (FakeServer server = tlsServer(other);
             Transport transport = new Transport(options(server, "localhost", dir.resolve("other.pem").toFile()))) {
            // The certificate is trusted, but it is not for localhost.
            assertThatThrownBy(transport::connection)
                    .isInstanceOf(ConnectionClosedException.class)
                    .hasMessageContaining("could not connect");
        }
    }

    @Test
    void aCertificateThatIsNotTrustedIsRefused() throws Exception {
        try (FakeServer server = tlsServer(matching);
             Transport transport = new Transport(options(server, "localhost", null))) {
            assertThatThrownBy(transport::connection).isInstanceOf(ConnectionClosedException.class);
        }
    }

    @Test
    void aPlaintextClientCannotTalkToATlsServer() throws Exception {
        try (FakeServer server = tlsServer(matching);
             Transport transport = new Transport(new ConnectOptions("localhost", server.port(), "t", "c", null,
                     null, 1000, 0, 0, 0))) {
            assertThatThrownBy(transport::connection).isInstanceOf(ConnectionClosedException.class);
        }
    }
}
