# MilekatUtils

MilekatUtils is a Java utility library developed by Milekat, designed to be shared across Minecraft (Spigot/Bukkit) plugins.
It provides a unified storage abstraction (MySQL, MariaDB, PostgreSQL, Elasticsearch), a messaging abstraction (RabbitMQ), and a set of general-purpose helpers for date, config, logging, and Minecraft utilities.

## Features

- **Storage subsystem** — unified `StorageConnection` interface over SQL and Elasticsearch backends
  - MySQL, MariaDB, PostgreSQL via HikariCP connection pooling
  - Elasticsearch 9.x with API key / password auth and optional SSL fingerprint
  - Schema loading from classpath resources
  - Dynamic adapter detection: only adapters whose driver is on the classpath are loaded

- **Messaging subsystem** — unified `MessagingConnection` interface over RabbitMQ
  - **Topic mode** (`registerMessageProcessor`) — pub/sub fan-out, every subscriber matching a routing key pattern receives every message
  - **Task queue mode** (`registerTaskProcessor`) — competing consumers, each message delivered to exactly one worker; workers can be paused/resumed with `setProcessorActive` without reconnecting
  - Auto-recovery after reconnect

- **Core utilities**
  - `Configs` — YAML config reader with dot-notation paths and Minecraft color code translation
  - `MileLogger` — logging wrapper with optional debug mode
  - `DateMileKat` — date formatting, duration, and period parsing (`1d12h30m`)
  - `McTools` / `McNames` — tab-completion helpers, color code translation, UUID↔name resolution
  - `Tools` — string helpers, random string generation, emoji↔int conversion

- **Shaded dependencies** — `json`, `snakeyaml`, `HikariCP`, and `slf4j` are relocated under `fr.milekat.utils.lib.*` to avoid classpath conflicts

## Requirements

- **Java** 11+
- **Maven / Gradle** project

## Installation

The library is published on **[Maven Central](https://central.sonatype.com/artifact/fr.milekat/milekat-utils)** and on GitHub Packages.

### Gradle

```gradle
dependencies {
    implementation 'fr.milekat:milekat-utils:VERSION'
}
```

### Maven

```xml
<dependency>
    <groupId>fr.milekat</groupId>
    <artifactId>milekat-utils</artifactId>
    <version>VERSION</version>
</dependency>
```

> **GitHub Packages** is also available as an alternative registry.
> It requires a GitHub personal access token with `read:packages` scope and the repository URL `https://maven.pkg.github.com/tutur1004/MilekatUtils`.

### Optional runtime dependencies

These are declared `compileOnly` — you must provide whichever ones your chosen backend needs at runtime:

| Backend | Dependency |
|---|---|
| MySQL | `com.mysql:mysql-connector-j` |
| MariaDB | `org.mariadb.jdbc:mariadb-java-client` |
| PostgreSQL | `org.postgresql:postgresql` |
| Elasticsearch | `co.elastic.clients:elasticsearch-java` + `com.fasterxml.jackson.core:jackson-databind` |
| RabbitMQ | `com.rabbitmq:amqp-client` |

## Configuration

Both subsystems read from a `Configs` instance backed by a YAML file.

### Storage

```yaml
storage:
  type: mysql        # mysql | mariadb | postgresql | elasticsearch
  sql:
    prefix:    minecraft_
    hostname:  localhost
    port:      3306
    database:  minecraft
    username:  root
    password:  ""
  elasticsearch:
    prefix:    minecraft-
    scheme:    http
    hostname:  localhost
    port:      9200
    username:  ""
    password:  ""
    apiKey:    ""
    sslFingerprint: ""
```

### Messaging

```yaml
messaging:
  type: rabbitmq     # rabbitmq
  rabbitmq:
    hostname:  localhost
    port:      5672
    vhost:     /
    username:  guest
    password:  guest
    exchange:  milekat.exchange
    type:      x-rtopic
```

## Usage

### Storage

```java
Configs configs = new Configs(new File("config.yml"));
MileLogger logger = new MileLogger("MyPlugin", false);

StorageLoader loader = new StorageLoader(StorageConfig.fromConfig(configs), logger);
StorageConnection storage = loader.getLoadedConnection();

// SQL — obtain a pooled connection
SQLConnection sql = (SQLConnection) storage;
try (Connection con = sql.getSQLClient().getConnection()) {
    // run queries
}

// Elasticsearch — obtain a typed client
ESConnection es = (ESConnection) storage;
ElasticsearchClient client = es.getEsClient();

// Load a SQL schema from classpath
storage.loadSchema(getClass().getResourceAsStream("/schema.sql"));

// Health check
boolean ok = storage.checkStoragesConnection();

// Cleanup
storage.close();
```

### Messaging — topic mode (pub/sub)

Every subscriber matching the routing key pattern receives every published message.

```java
MessagingLoader loader = new MessagingLoader(configs, logger);
MessagingConnection messaging = loader.getLoadedMessaging();

// Subscribe
messaging.registerMessageProcessor("my-processor", "events.#", msg -> {
    System.out.println("Received on " + msg.getRoutingKey() + ": " + msg.getMessage());
    msg.ack();
});

// Publish
messaging.sendMessage("events.player.join", "{\"player\":\"Notch\"}");

// Unsubscribe
messaging.unregisterMessageProcessor("my-processor");
```

### Messaging — task queue mode (competing consumers)

All workers share a single durable queue. Each message is delivered to exactly one worker.
Workers start **inactive** and must be explicitly enabled.

```java
// Register a task worker (starts paused)
messaging.registerTaskProcessor("worker-1", "jobs.encode", msg -> {
    System.out.println("Processing job: " + msg.getMessage());
    msg.ack();
});

// Activate when ready to accept work
messaging.setProcessorActive("worker-1", true);

// Pause (no new messages delivered, channel kept open)
messaging.setProcessorActive("worker-1", false);

// Send a task — delivered to exactly one active worker
messaging.sendMessage("jobs.encode", "{\"file\":\"video.mp4\"}");
```

### Configs

```java
Configs configs = new Configs(new File("config.yml"));

String host   = configs.getString("storage.sql.hostname", "localhost");
int    port   = configs.getInt("storage.sql.port", 3306);
String prefix = configs.getString("storage.sql.prefix", "mc_");

// Minecraft color codes (&a, &b …) are translated automatically
String motd = configs.getMessage("server.motd");
```

### MileLogger

```java
MileLogger logger = new MileLogger("MyPlugin", false); // second arg = debug mode
logger.info("Plugin started");
logger.debug("Verbose detail");   // only printed when debug = true
logger.warn("Something odd");
logger.error("Something failed");
```

### DateMileKat

```java
// Format a date
String formatted = DateMileKat.getDateCtm();            // current time, custom format
String es         = DateMileKat.getDateEs(someDate);    // Elasticsearch-compatible

// Parse a period string
long millis = DateMileKat.parsePeriod("1d12h30m");      // → milliseconds

// Human-readable remaining time
String remaining = DateMileKat.remainingToString(futureDate); // "2d 3h 15m"
```

## Package Structure

```
fr.milekat.utils
├── Configs
├── DateMileKat
├── McNames
├── McTools
├── MileLogger
├── Tools
├── messaging/
│   ├── MessagingConnection       (interface)
│   ├── MessagingLoader
│   ├── MessagingVendor           (RABBITMQ, REDIS)
│   ├── ReceivedMessage           (interface)
│   ├── MessagingChanel
│   └── adapter/
│       ├── rabbitmq/RabbitMQConnection
│       └── redis/RedisConnection
└── storage/
    ├── StorageConnection         (interface)
    ├── StorageLoader
    ├── StorageVendor             (MYSQL, MARIADB, POSTGRESQL, ELASTICSEARCH)
    └── adapter/
        ├── sql/connection/SQLConnection
        ├── sql/hikari/HikariEngine
        └── elasticsearch/connection/ESConnection
```

## Build

```bash
# Build (produces a shaded/fat JAR)
JAVA_HOME="/c/Users/arthu/.jdks/corretto-21.0.6" ./gradlew build

# Run tests
JAVA_HOME="/c/Users/arthu/.jdks/corretto-21.0.6" ./gradlew test

# Publish to GitHub Packages
JAVA_HOME="/c/Users/arthu/.jdks/corretto-21.0.6" ./gradlew publish
```

## Credits

- **Developer:** Milekat — [GitHub](https://github.com/tutur1004)

## Support

Report issues at [MilekatUtils GitHub Issues](https://github.com/tutur1004/MilekatUtils/issues).
