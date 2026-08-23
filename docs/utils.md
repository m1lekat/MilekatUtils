# Utils — MilekatUtils

General utility classes, **no Bukkit dependency**. Usable on both Velocity and Bukkit to share common code.

---

## MileLogger

Wrapper around `java.util.logging.Logger` with a debug mode. Passed to `StorageLoader` / `MessagingLoader`.

```java
// Standalone (creates its own console handler with timestamp)
MileLogger logger = new MileLogger("MyPlugin");
MileLogger logger = new MileLogger("MyPlugin", true); // debug enabled

// Wrap an existing logger (Bukkit / Velocity)
MileLogger logger = new MileLogger(plugin.getSLF4JLogger()); // Velocity
MileLogger logger = new MileLogger(plugin.getLogger());      // Bukkit
```

| Method | Behavior |
|---|---|
| `info(msg)` | Log at INFO level |
| `warning(msg)` | Log at WARNING level |
| `debug(msg)` | INFO log prefixed with `[DEBUG]` — **no-op if debug = false** |
| `stack(StackTraceElement[])` | Prints stacktrace — **no-op if debug = false** |
| `setDebug(bool)` | Enables/disables debug mode at runtime |

---

## Configs

YAML reader (SnakeYAML) with dot notation. Used by Storage and Messaging to read their configuration.

```java
Configs config = new Configs(new File(dataFolder, "config.yml"));

config.getString("storage.type");           // "" if missing
config.getString("storage.type", "mysql");  // default if missing
config.getInt("storage.sql.port", 3306);
config.getLong("some.ttl", 86400L);
config.getDouble("some.rate", 1.0);
config.getBoolean("feature.enabled", false);
config.getStringList("some.list");

// Static read from an already loaded Map
Configs.getNodeValue("storage.type", yamlMap);
```

**Minecraft methods** (translates color codes `&x` → `§x`) :

```java
config.getMessage("messages.welcome");           // String with color codes
config.getMessage("messages.welcome", "Welcome!");
config.getMessages("messages.motd");             // List<String>
```

---

## DateMileKat

Date utilities, including a natural-language duration parser.

### Predefined formats

| Method | Format | Example |
|---|---|---|
| `getDateCtm()` | `dd/MM/yyyy HH:mm:ss` | `22/04/2026 14:30:00` |
| `getDateSys()` | `yyyy-MM-dd_HH-mm-ss` | `2026-04-22_14-30-00` |
| `getDateEs()` | `yyyy-MM-dd'T'HH:mm:ss.SSSZ` | `2026-04-22T14:30:00.000+0200` |

Each format has a `getDate*(Date)` variant (format a given date) and `getDate*()` (current date), plus `get*StringDate(String)` to parse.

```java
String now = DateMileKat.getDateEs();               // now → ES format
Date parsed = DateMileKat.getESStringDate(str);     // parse ES → Date
String display = DateMileKat.getDateCtm(someDate);  // Date → display in CTM format
```

### Remaining time

```java
// Duration between two dates → map D/h/m/s/ms
HashMap<String, String> remaining = DateMileKat.getReamingTime(futureDate, new Date());
// keys: "D" (days), "h", "m", "s", "ms" (values can be negative if date1 < date2)

// Formatted as a readable string ("2days 3h 15m")
String label = DateMileKat.reamingToString(futureDate);
```

### Duration parser (user config)

Parses a string like `"1d12h30m"` and returns milliseconds since epoch 0.

```java
long ms = DateMileKat.parsePeriod("30m");    // 1_800_000
long ms = DateMileKat.parsePeriod("1j12h");  // supports "j" and "d" for days
long ms = DateMileKat.parsePeriod("2d30s");

// Typical use: compute an expiration date
Date expiry = new Date(System.currentTimeMillis() + DateMileKat.parsePeriod("7d"));
```

Supported units: `s` (seconds), `m` (minutes), `h` (hours), `d` / `j` (days).

---

## McNames

Resolve name ↔ UUID via Mojang API. Blocking (synchronous HTTP call).

```java
String uuid = McNames.getUuid("Notch");   // "069a79f4-44e9-4726-a5be-fca90e38aaf5"
                                           // "invalid name" if player does not exist
                                           // "error" if network error

String name = McNames.getName("069a79f4-44e9-4726-a5be-fca90e38aaf5"); // "Notch"
                                                                         // "invalid uuid" / "error"
```

Call from an async thread — the HTTP request can take several hundred ms.

---

## McTools

Command autocomplete helper (works equally on Velocity and Bukkit).

```java
// Filter a list based on what the player has already typed
List<String> suggestions = McTools.getTabArgs(arg, List.of("start", "stop", "status"));
// If arg = "st" → ["start", "stop", "status"]
// If arg = "sta" → ["start", "status"]
// Case-insensitive
```

---

## Tools

General utilities with no Minecraft context.

```java
Tools.remLastChar("hello ");          // "hello" — removes the last character
Tools.isAlphaNumericExtended("ab_1"); // true — [a-z A-Z 0-9 space () _ é è ê ï ç -]
Tools.getRandomString(16);            // e.g.: "aZ3Kp9XqRm7NvT2w"
Tools.toByteArray(inputStream);       // InputStream → byte[]

// Integer ↔ Discord/unicode emoji conversion (1–9)
String emoji = Tools.getString(3); // "3️⃣"
int num = Tools.getInt("3️⃣");      // 3
```
