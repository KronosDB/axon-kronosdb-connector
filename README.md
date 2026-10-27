# Axon KronosDB Connector

An [Axon Framework](https://github.com/AxonFramework/AxonFramework) 5 connector for [KronosDB](https://github.com/KronosDB/kronosdb): event store, snapshot store, command bus and query bus (including subscription queries) over KronosDB's native protocol.

Axon Framework applications can use KronosDB as their infrastructure backbone, in place of Axon Server. The connector implements Axon's infrastructure interfaces on the native protocol: one TCP connection per application instance, multiplexing every request and stream, on the port the server listens on for clients (`--listen`, default `50051`), with or without TLS.

## Requirements

| | |
|---|---|
| Axon Framework | 5.3 or later (built against 5.4.1; CI runs 5.3.0, 5.3.3 and 5.4.1) |
| KronosDB | 0.11 or later |
| Java | 21 |

**Versions before 0.6.0** are the gRPC connector, for KronosDB 0.10 and earlier. KronosDB 0.11 replaced gRPC with the native protocol, which is a breaking change, so 0.6.0 is the first connector for 0.11 and the older ones do not work against it. If you run KronosDB 0.10 or earlier, stay on [v0.1.0](https://github.com/KronosDB/axon-kronosdb-connector/tree/v0.1.0), the last gRPC release.

## Install

Public builds come from [JitPack](https://jitpack.io), built from this repository's Git tags. Add the repository and the dependency, plus the Axon modules, which the connector does not pull in for you so that your application picks the version:

```xml
<repositories>
    <repository>
        <id>jitpack.io</id>
        <url>https://jitpack.io</url>
    </repository>
</repositories>

<dependencies>
    <dependency>
        <groupId>com.github.KronosDB</groupId>
        <artifactId>axon-kronosdb-connector</artifactId>
        <version>v0.6.0</version>
    </dependency>
    <dependency>
        <groupId>org.axonframework</groupId>
        <artifactId>axon-messaging</artifactId>
        <version>5.4.1</version>
    </dependency>
    <dependency>
        <groupId>org.axonframework</groupId>
        <artifactId>axon-eventsourcing</artifactId>
        <version>5.4.1</version>
    </dependency>
</dependencies>
```

Gradle:

```kotlin
repositories {
    maven("https://jitpack.io")
}

dependencies {
    implementation("com.github.KronosDB:axon-kronosdb-connector:v0.6.0")
    implementation("org.axonframework:axon-messaging:5.4.1")
    implementation("org.axonframework:axon-eventsourcing:5.4.1")
}
```

Axon 5.4's default converter is Jackson-based, so an application that relies on the default needs `jackson-databind` too (as it would without this connector).

## Use

With the connector on the classpath, Axon discovers `KronosDbConfigurationEnhancer` through the `ServiceLoader` and wires the KronosDB-backed event store, snapshot store, command bus connector and query bus connector. Supply the connection settings by registering a `KronosDbConfiguration`:

```java
KronosDbConfiguration kronos = KronosDbConfiguration.builder()
        .servers("localhost:50051")
        .context("default")
        .componentName("MyApplication")
        .build();

AxonConfiguration app = EventSourcingConfigurer.create()
        .componentRegistry(r -> r.registerComponent(KronosDbConfiguration.class, c -> kronos))
        // ... your command, query and event handling modules
        .build();
app.start();
```

`src/test/java/io/kronosdb/connector/axon/AxonApplicationSmokeServerTest.java` is a complete, running example: a command, a query, a subscription query and an event through a real Axon application.

To wire the pieces by hand instead:

```java
var connectionManager = KronosDbConnectionManager.builder().configuration(kronos).build();
connectionManager.start();
var connection = connectionManager.getConnection();

var eventStore = new KronosDbEventStorageEngine(connection, eventConverter);
var snapshotStore = new KronosDbSnapshotStore(connection, generalConverter);
var commandConnector = new KronosDbCommandBusConnector(connection, kronos, messageConverter);
var queryConnector = new KronosDbQueryBusConnector(connection, kronos, messageConverter);
```

## Supported features

| Feature | Axon interface | Native protocol |
|---------|---------------|-----------------|
| Event store | `EventStorageEngine` | Append, Source, Subscribe, Head/Tail, Tags |
| Snapshot store | `SnapshotStore` | AppendSnapshot, GetSnapshot |
| Command bus | `CommandBusConnector` | DispatchCommand, OpenCommandHandlers |
| Query bus | `QueryBusConnector` | DispatchQuery, OpenQueryHandlers, OpenSubscriptionQuery |
| Connection management | `KronosDbConnectionManager` | GetPlatformInfo, OpenPlatform (heartbeats, processor status and control) |

Not supported:

- `tokenAt(Instant)`: KronosDB has no time-based sequence lookup, so it falls back to `latestToken()`.
- Event scheduling: the connector does not expose KronosDB's scheduler.

## The distributed command and query bus is vendored

Axon Framework 5.1 moved the distributed command bus, the distributed query bus and the `CommandBusConnector` / `QueryBusConnector` contracts out of the open-source `axon-messaging` artifact and into the commercial Axoniq Framework. A connector that plugs in a command and query transport needs exactly these, so this repository carries its own copy of the last Apache-2.0 versions: twelve files, unchanged except for their package, in `io.kronosdb.connector.axon.distributed.command` and `.query`.

- You do not need Axoniq Framework, and this connector never depends on it.
- The copies are registered through their own `ConfigurationEnhancer`s (`META-INF/services`), so the `CommandBus` and `QueryBus` an Axon application builds are the vendored `DistributedCommandBus` and `DistributedQueryBus` talking to KronosDB.
- They are distinct types from Axoniq Framework's own, in a different package: a connector written against the Axoniq interfaces is not interchangeable with this one.
- Where each file came from, and the license terms, are in [`NOTICE`](NOTICE). Each file keeps its original Apache License 2.0 header and starts with a comment naming its source file and original package.

## Configuration

| Property | Default | Description |
|----------|---------|-------------|
| `servers` | `localhost:50051` | KronosDB server address (host:port) |
| `context` | `default` | Default context name |
| `busName` | `null` | Messaging bus for commands/queries/subscriptions; independent of context, `null` uses the server's `default` bus |
| `componentName` | `Unnamed` | Application component name |
| `clientId` | `<random UUID>` | Unique client instance identifier. Two live instances must not share one: the server ends the older of two handler streams of a client id |
| `token` | `null` | Authentication token (a static token or a JWT; sent as credential kind `token`) |
| `sslEnabled` | `false` | Enable TLS on the server's port (there is no plaintext fallback) |
| `certFile` | `null` | PEM file with the server's certificate or its CA; the JVM's trust store is used if unset |
| `keepAliveTime` | `1000` | Ping a connection silent for this long (ms); 0 relies on the server's pings |
| `keepAliveTimeout` | `5000` | Close the connection if nothing at all arrives this long after a ping (ms) |
| `heartbeatEnabled` | `true` | Send periodic platform heartbeats (the platform stream also answers the server's) |
| `heartbeatInterval` | `5000` | Heartbeat interval (ms) |
| `maxMessageSize` | `4MB` | Largest frame to send or accept; can only lower the limit the server announces |
| `queryTimeoutMillis` | `0` | How long a query may wait for its answer; `0` sends none, so the server's `[timeouts] query` (30 s by default) applies |

## Building

```bash
mvn verify
```

Needs Java 21 and Maven 3.9+, and nothing but Maven Central: the native protocol codec is vendored into this repository (below). To build against another Axon version, `mvn verify -Daxon.version=5.3.0`. JDK 25 is not supported by the test dependencies (Mockito's inline agent), so use 21.

## Tests

`mvn verify` runs everything that needs no server: a scripted stand-in server, TLS with generated certificates, and the protocol conformance suite. The tests that need KronosDB are skipped unless a server is named, so they show up as skipped, not as passed.

### Running the server tests

Start a KronosDB 0.11+ server with the short timeouts the timeout and heartbeat tests rely on, from a KronosDB checkout or the `ghcr.io/kronosdb/kronosdb` image:

```bash
kronosdb-server --dev --listen 127.0.0.1:50051 --admin-listen 127.0.0.1:9240 \
    --command-timeout 3 --query-timeout 3 --heartbeat-interval 1 --heartbeat-timeout 4
```

Then:

```bash
mvn verify -Dkronosdb.native.addr=localhost:50051 \
           -Dkronosdb.native.admin=localhost:9240 \
           -Dkronosdb.native.short-timeouts=true
```

- `kronosdb.native.addr`: the server (`host:port`): event store, commands, queries, subscription queries, platform, Axon connectors.
- `kronosdb.native.admin`: its admin HTTP address, for the tests that look at registered clients and processors and send processor instructions.
- `kronosdb.native.short-timeouts`: the server was started with the short timeouts above.

`AuthNativeServerTest` needs a second server with authentication on. [`scripts/test-auth.toml`](scripts/test-auth.toml) is its config (throwaway tokens):

```bash
kronosdb-server --dev --config scripts/test-auth.toml --listen 127.0.0.1:50052 --admin-listen 127.0.0.1:9241 --data-dir auth-data
```

```bash
mvn verify ... -Dkronosdb.native.auth.addr=localhost:50052 \
               -Dkronosdb.native.auth.full=full-token \
               -Dkronosdb.native.auth.reader=reader-token \
               -Dkronosdb.native.auth.writer=writer-token
```

Tests name their commands, queries and tags uniquely, so they can share a server with each other.

### CI

`.github/workflows/build.yml` runs `mvn verify` on JDK 21 for Axon 5.3.0, 5.3.3 and 5.4.1. The server tests run in a second job that starts the servers above from `ghcr.io/kronosdb/kronosdb:0.11.0`, the oldest supported server; a manual dispatch can name another `kronosdb-image`.

## The native protocol codec is vendored

The wire format is generated in the KronosDB repository from `protocol/kronosdb.schema`. So that this repository builds from Maven Central alone, the generated Java codec is copied here instead of being a dependency:

| Path | What it is |
|---|---|
| `src/main/java/io/kronosdb/protocol/` | The codec, generated, never edited here; ships in the connector jar |
| `protocol/kronosdb.schema` | The schema it was generated from |
| `protocol/fixtures/` | The golden frames and their canonical JSON |
| `src/test/java/io/kronosdb/protocol/` | `ConformanceTest` (every fixture decodes and re-encodes byte for byte), its JSON bridge, and `SchemaHashTest` |

`scripts/vendor-protocol.sh <path to a KronosDB checkout>` refreshes all of it. It prints the new `SCHEMA_HASH`; pin that in `SchemaHashTest.PINNED_SCHEMA_HASH`. That one-line change is the review point for "the wire changed", and the conformance suite proves the new codec matches the new fixtures. Vendored with schema hash `a65e0eff`, from KronosDB 0.11.

## License

Apache License 2.0, see [`LICENSE`](LICENSE). Third-party code included in this repository, and its provenance, is listed in [`NOTICE`](NOTICE).
