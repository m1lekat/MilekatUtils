# Storage — MilekatUtils

Abstraction over SQL (MySQL/MariaDB/PostgreSQL) and Elasticsearch.  
The correct adapter is picked **automatically** at runtime based on which driver JAR is on the classpath.

---

## Dependency (consumer project)

```xml
<dependency>
    <groupId>fr.milekat</groupId>
    <artifactId>milekat-utils</artifactId>
    <version>1.8.0</version>
</dependency>
```

The consumer project must also declare the desired driver as a runtime dependency (they are `compileOnly` in this lib):

| Storage type | Driver artifact |
|---|---|
| MySQL | `com.mysql:mysql-connector-j` |
| MariaDB | `org.mariadb.jdbc:mariadb-java-client` |
| PostgreSQL | `org.postgresql:postgresql` |
| Elasticsearch | `co.elastic.clients:elasticsearch-java` + `com.fasterxml.jackson.core:jackson-databind` |

---

## Config YAML

### SQL (MySQL / MariaDB / PostgreSQL)

```yaml
storage:
  type: mysql          # mysql | mariadb | postgres | postgresql
  prefix: myapp_       # table prefix, replaces {prefix} in schema files
  sql:
    hostname: localhost
    port: "3306"
    database: mydb
    username: user
    password: secret
```

### Elasticsearch

```yaml
storage:
  type: elasticsearch  # es | elastic | elasticsearch
  elasticsearch:
    method: http       # http | https  (default: http)
    hostname: localhost
    port: 9200         # default: 9200
    api_key: ""        # use either api_key OR username+password
    username: elastic
    password: secret
    ssl_fingerprint: "" # optional: CA fingerprint for SSL pinning
```

---

## Loading a connection

```java
Configs config = new Configs(new File("config.yml"));
MileLogger logger = new MileLogger("MyPlugin");

StorageConnection storage;
try {
    storage = new StorageLoader(config, logger).getLoadedConnection();
} catch (StorageLoadException e) {
    // driver not on classpath, bad config, or connection refused
}
```

`StorageLoader` verifies the connection via `checkStoragesConnection()` before returning.  
Always call `storage.close()` on plugin disable.

---

## SQL — SQLConnection

Cast `StorageConnection` to `SQLConnection` to access SQL-specific features.

```java
SQLConnection sql = (SQLConnection) storage;
```

### Execute queries

```java
try (Connection conn = sql.getSQLClient().getConnection()) {
    try (PreparedStatement ps = conn.prepareStatement(
            "SELECT * FROM " + sql.getPrefix() + "players WHERE uuid = ?")) {
        ps.setString(1, uuid.toString());
        ResultSet rs = ps.executeQuery();
        // ...
    }
}
```

- `getConnection()` throws `StorageLoadException` if the pool is closed or exhausted.
- Always use try-with-resources — connections return to the pool on close.
- `getPrefix()` returns the raw prefix string (e.g. `"myapp_"`).

### Load a schema file

Schema is applied once at startup (idempotent — "already exists" errors are silently ignored).  
In SQL files, use `{prefix}` as a placeholder; it is replaced by the configured prefix.

```sql
-- schema.sql
CREATE TABLE {prefix}players (
    uuid VARCHAR(36) PRIMARY KEY,
    name VARCHAR(16) NOT NULL
);
```

```java
InputStream schema = getClass().getResourceAsStream("/schema.sql");
sql.loadSchema(schema);
```

---

## Elasticsearch — ESConnection

Cast `StorageConnection` to `ESConnection` to get the raw client.

```java
ESConnection es = (ESConnection) storage;
ElasticsearchClient client = es.getEsClient();

// With custom Jackson mapper (needed for Minecraft type serializers):
JacksonJsonpMapper mapper = new JacksonJsonpMapper();
SimpleModule module = new SimpleModule();
module.addSerializer(/* ... */);
mapper.objectMapper().registerModule(module);
ElasticsearchClient client = es.getEsClient(mapper);
```

### Index — auto-create on startup

`Index` checks whether the index exists and creates it with **strict dynamic mapping** if not.

```java
// Without tags
Map<String, Class<?>> fields = new LinkedHashMap<>();
fields.put("playerUuid", String.class);
fields.put("timestamp", Date.class);
fields.put("score", Integer.class);

new Index(client, "my-index", "1", fields);

// With tags (nested object field)
Map<String, Class<?>> tags = new LinkedHashMap<>();
tags.put("server", String.class);
tags.put("world", String.class);

new Index(client, "my-index", "1", fields, tags, "tags");
// → creates a nested "tags" field containing server + world keyword fields
```

Supported Java → ES type mappings (from `Mapping`):

| Java type | ES mapping |
|---|---|
| `String.class` | `keyword` |
| `Integer.class` / `Long.class` | `integer` / `long` |
| `Double.class` / `Float.class` | `double` / `float` |
| `Boolean.class` | `boolean` |
| `Date.class` | `date` |

### Transforms — auto-create + auto-start

Used to maintain aggregated indices. Two modes:

```java
// PIVOT — sum aggregation grouped by tag fields
Map<String, Class<?>> pivotGroups = new LinkedHashMap<>();
pivotGroups.put("server", String.class);

new Transforms(client,
    "my-index",           // source index
    "my-index-by-server", // destination index
    "@timestamp",         // sync field
    "60s",                // sync delay
    "1m",                 // frequency
    pivotGroups           // pivot group-by fields (prefixed with "tags.")
);

// LATEST — keep most recent doc per unique key
new Transforms(client,
    "my-index",
    "my-index-latest",
    "@timestamp",
    "60s",
    "1m",
    List.of("playerUuid"),  // unique key fields
    "@timestamp"            // sort field
);
```

Transform ID is generated as `<destinationIndex>-<firstField>` (lowercase).  
If the transform already exists and is started, nothing happens.

### Built-in Minecraft mappers

Available in `fr.milekat.utils.storage.adapter.elasticsearch.mappers`:

- `MinecraftMappers` — registers all Minecraft serializers/deserializers on a mapper
- `BlockSerializer` / `BlockDeserializer` — Bukkit `Block`
- `InventorySerializer` / `InventoryDeserializer` — Bukkit `Inventory`
- `BukkitConfigurationSerializer` / `BukkitConfigurationDeserializer` — `ConfigurationSerializable`
- `DateSerializer` / `DateDeserializer` — `java.util.Date`

```java
JacksonJsonpMapper mapper = new JacksonJsonpMapper();
MinecraftMappers.register(mapper.objectMapper());
ElasticsearchClient client = es.getEsClient(mapper);
```
