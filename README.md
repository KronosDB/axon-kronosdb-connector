# Axon KronosDB Connector

An Axon Framework 5 connector for [KronosDB](https://github.com/kronosdb/kronosdb) — providing event store, snapshot store, command bus, and query bus integration over gRPC.

## Overview

This library allows Axon Framework 5 applications to use KronosDB as their infrastructure backbone, replacing the need for Axon Server. It implements Axon Framework's core infrastructure interfaces backed by KronosDB's gRPC API.

### Supported Features

| Feature | Axon Interface | KronosDB Service |
|---------|---------------|-----------------|
| Event Store | `EventStorageEngine` | `EventStore` gRPC |
| Snapshot Store | `SnapshotStore` | `SnapshotStore` gRPC |
| Command Bus | `CommandBusConnector` | `CommandService` gRPC |
| Query Bus | `QueryBusConnector` | `QueryService` gRPC |
| Connection Management | `KronosDbConnectionManager` | `PlatformService` gRPC |

### Not Supported

- `tokenAt(Instant)` — KronosDB does not have a time-based sequence lookup. Falls back to `latestToken()`.
- Event scheduling — KronosDB does not have a built-in event scheduler.

## Quick Start

```java
// 1. Configure the connection
var config = KronosDbConfiguration.builder()
        .servers("localhost:50051")
        .context("default")
        .componentName("MyApplication")
        .build();

// 2. Create the connection manager
var connectionManager = KronosDbConnectionManager.builder()
        .configuration(config)
        .build();
connectionManager.start();

// 3. Get a connection and create components
var connection = connectionManager.getConnection();

// Event Store
var eventStore = new KronosDbEventStorageEngine(connection, eventConverter);

// Snapshot Store
var snapshotStore = new KronosDbSnapshotStore(connection, converter);

// Command Bus
var commandConnector = new KronosDbCommandBusConnector(connection, config);

// Query Bus
var queryConnector = new KronosDbQueryBusConnector(connection, config);
```

## Project Structure

```
src/main/
├── proto/                          # KronosDB protobuf definitions
│   ├── eventstore.proto
│   ├── snapshot.proto
│   ├── command.proto
│   ├── query.proto
│   ├── platform.proto
│   └── common.proto
└── java/io/kronosdb/connector/
    ├── grpc/                       # Low-level gRPC client layer
    │   ├── KronosDbConnection      # Per-context connection
    │   ├── KronosDbConnectionFactory
    │   ├── EventStoreChannel       # Event store operations
    │   ├── SnapshotChannel         # Snapshot operations
    │   ├── CommandChannel          # Command dispatch/handling
    │   ├── QueryChannel            # Query dispatch/handling
    │   ├── PlatformChannel         # Heartbeat & lifecycle
    │   ├── ResultStream            # Async pull-based stream
    │   └── Registration            # Subscription management
    └── axon/                       # Axon Framework integration
        ├── KronosDbConfiguration
        ├── KronosDbConnectionManager
        ├── ErrorCode
        ├── MetadataConverter
        ├── event/
        │   ├── KronosDbEventStorageEngine
        │   ├── TaggedEventConverter
        │   ├── ConditionConverter
        │   ├── SourcingEventMessageStream
        │   └── StreamingEventMessageStream
        ├── snapshot/
        │   └── KronosDbSnapshotStore
        ├── command/
        │   ├── KronosDbCommandBusConnector
        │   └── CommandConverter
        └── query/
            ├── KronosDbQueryBusConnector
            ├── QueryConverter
            ├── QueryResponseMessageStream
            └── SubscriptionQueryResponseMessageStream
```

## Building

```bash
mvn clean compile    # Compile (generates protobuf code)
mvn clean install    # Build and install to local Maven repo
```

Requires:
- Java 21+
- Maven 3.9+
- Axon Framework 5.1.0

Axon Framework 5.1.0 moved the distributed command/query connector APIs to Axoniq Framework. This connector vendors the small Apache-licensed connector contracts and payload-converting decorators from Axon Framework 5.0.5 so downstream projects do not need Axoniq Framework just to use the KronosDB connector.

## Published Package

Packages are published to GitHub Packages from the `main` branch and `v*` tags.

Maven:

```xml
<repositories>
    <repository>
        <id>github</id>
        <url>https://maven.pkg.github.com/KronosDB/axon-kronosdb-connector</url>
    </repository>
</repositories>

<dependencies>
    <dependency>
        <groupId>io.kronosdb</groupId>
        <artifactId>axon-kronosdb-connector</artifactId>
        <version>0.1.0-SNAPSHOT</version>
    </dependency>
</dependencies>
```

Gradle:

```kotlin
repositories {
    maven {
        url = uri("https://maven.pkg.github.com/KronosDB/axon-kronosdb-connector")
        credentials {
            username = findProperty("gpr.user") as String? ?: System.getenv("GITHUB_ACTOR")
            password = findProperty("gpr.key") as String? ?: System.getenv("GITHUB_TOKEN")
        }
    }
}

dependencies {
    implementation("io.kronosdb:axon-kronosdb-connector:0.1.0-SNAPSHOT")
}
```

GitHub Packages may require a GitHub token with `read:packages` when resolving packages from another project.

## Configuration

| Property | Default | Description |
|----------|---------|-------------|
| `servers` | `localhost:50051` | KronosDB server address (host:port) |
| `context` | `default` | Default context name |
| `componentName` | `Unnamed` | Application component name |
| `clientId` | `<random UUID>` | Unique client instance identifier |
| `token` | `null` | Authentication token |
| `sslEnabled` | `false` | Enable TLS |
| `certFile` | `null` | TLS certificate file path |
| `heartbeatEnabled` | `true` | Enable heartbeat |
| `heartbeatInterval` | `5000` | Heartbeat interval (ms) |
| `maxMessageSize` | `4MB` | Maximum gRPC message size |

## License

Apache License 2.0
