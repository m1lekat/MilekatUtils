# Messaging — MilekatUtils

Abstraction over message brokers. Currently implemented: **RabbitMQ**.  
Redis adapter is declared (`MessagingVendor.REDIS`) but not yet implemented (stub class only).

The correct adapter is picked at runtime based on which driver JAR is on the classpath.

---

## Dependency (consumer project)

```xml
<dependency>
    <groupId>fr.milekat</groupId>
    <artifactId>milekat-utils</artifactId>
    <version>1.8.0</version>
</dependency>
```

Declare the broker client as a runtime dependency (it is `compileOnly` in this lib):

| Vendor | Driver artifact |
|---|---|
| RabbitMQ | `com.rabbitmq:amqp-client` |
| Redis | `redis.clients:jedis` (not yet implemented) |

---

## Config YAML — RabbitMQ

```yaml
messaging:
  type: rabbitmq
  rabbitmq:
    hostname: localhost
    port: 5672           # default: 5672
    vhost: /             # default: /
    username: guest
    password: guest
    exchange: milekat.exchange   # default: milekat.exchange
    type: x-rtopic               # exchange type, default: x-rtopic
```

---

## Loading a connection

```java
Configs config = new Configs(new File("config.yml"));
MileLogger logger = new MileLogger("MyPlugin");

MessagingConnection messaging = new MessagingLoader(config, logger).getLoadedMessaging();
```

`MessagingLoader` throws `IllegalArgumentException` if the type is unsupported or the connection fails.  
Always call `messaging.close()` on plugin disable.

---

## Sending messages

```java
// Simple send (no callback key)
messaging.sendMessage("target.routing.key", "hello world");

// Send with callback routing key (so the receiver knows where to reply)
messaging.sendMessage("target.routing.key", "my.callback.key", "hello world");
```

Messages are wrapped internally as JSON:
```json
{
  "TAG": "JSON_MESSAGE",
  "senderCallBackKey": "my.callback.key",
  "message": "hello world"
}
```

Only messages with `TAG = "JSON_MESSAGE"` are processed by consumers; others are silently acked and dropped.

---

## Receiving messages

### Register a processor

```java
// Auto-generated UUID as processor name, returns the name
String processorId = messaging.registerMessageProcessor("routing.key.#", msg -> {
    System.out.println("Received: " + msg.getMessage());
    System.out.println("From routing key: " + msg.getRoutingKey());
    System.out.println("Reply to: " + msg.getCallbackRoutingKey()); // nullable
    try {
        msg.ack(); // MUST call ack() or reject()
    } catch (IOException e) { /* handle */ }
});

// Named processor (preferred — needed for unregistration)
messaging.registerMessageProcessor("my-processor", "routing.key.#", msg -> {
    // ...
    msg.ack();
});
```

**Important:** every handler **must** call `msg.ack()` or `msg.reject()`.  
If neither is called, the lib auto-rejects the message and logs a warning.

### ReceivedMessage API

| Method | Description |
|---|---|
| `getMessage()` | The raw message payload (String) |
| `getRoutingKey()` | The routing key the message was received on |
| `getCallbackRoutingKey()` | Nullable — the sender's reply-to key |
| `ack()` | Acknowledge (remove from queue) |
| `reject()` | Reject without requeue |
| `isAcknowledged()` | Whether ack/reject was already called |

### Unregister a processor

```java
messaging.unregisterMessageProcessor("my-processor");
```

---

## Consumer modes

MilekatUtils supports two consumer modes. Choose the right one for each use case.

### Mode 1 — Topic (pub/sub)

_Registered with `registerMessageProcessor()`. Default mode._

Every subscriber matching the routing key pattern receives **every** message — classic fan-out.

| Property | Value |
|---|---|
| Exchange type | `x-rtopic` (configurable) |
| Queue per processor | Yes — unique, exclusive, auto-delete |
| Delivery | Copy to every matching subscriber |
| Use case | Event broadcasts, notifications |

```java
// Every worker subscribed to "server.events.#" gets a copy of each event
messaging.registerMessageProcessor("my-listener", "server.events.#", msg -> {
    System.out.println("Event: " + msg.getMessage());
    msg.ack();
});
```

### Mode 2 — Task queue (competing consumers)

_Registered with `registerTaskProcessor()`. New mode._

All workers share **one durable queue**. Each message is delivered to exactly **one** worker —
whichever is available first. Workers start **inactive** and must explicitly call
`setProcessorActive()` when ready to accept a task.

| Property | Value |
|---|---|
| Exchange | Default (direct queue) |
| Queue | Shared, durable, non-exclusive |
| Delivery | One worker only (competing consumers) |
| Prefetch | `basicQos(1)` — one message at a time per worker |
| Use case | Job processing, work queues |

```java
// Register the task processor (starts inactive)
messaging.registerTaskProcessor("my-worker", "job-queue", msg -> {
    process(msg.getMessage());
    msg.ack();
});

// Signal readiness — the worker starts receiving tasks
messaging.setProcessorActive("my-worker", true);

// Signal busy — no more tasks are pushed until reactivated
messaging.setProcessorActive("my-worker", false);
```

**Comparison:**

| | Topic | Task queue |
|---|---|---|
| Who gets the message? | All matching subscribers | Exactly one worker |
| Queue durability | Transient (auto-delete) | Durable (survives restart) |
| Starts active? | Yes | No — explicit `setProcessorActive` required |
| Use case | Fan-out / events | Work queues / job distribution |

---

## Queue behaviour (RabbitMQ)

**Topic processor** — each processor gets its own dedicated queue:

- **Durable:** false (does not survive broker restart)
- **Exclusive:** true (only this connection)
- **Auto-delete:** true (deleted when connection closes)

The queue is bound to the configured exchange using the provided routing key pattern.  
The `processorName` is used as the **queue name** — must be unique per connection.

**Task processor** — workers share a single queue:

- **Durable:** true (survives broker restart)
- **Exclusive:** false (shared across connections)
- **Auto-delete:** false (persists when workers disconnect)
- **Prefetch:** 1 (fair dispatch via `basicQos`)

---

## Task queue — n workers example

```java
// Worker A (e.g. on server-1)
MessagingConnection workerA = new MessagingLoader(config, logger).getLoadedMessaging();
workerA.registerTaskProcessor("job-worker", "render-jobs", msg -> {
    System.out.println("[A] Processing: " + msg.getMessage());
    doHeavyWork(msg.getMessage());
    msg.ack();
});

// Worker B (e.g. on server-2 — same queue name, different processor name)
MessagingConnection workerB = new MessagingLoader(config, logger).getLoadedMessaging();
workerB.registerTaskProcessor("job-worker", "render-jobs", msg -> {
    System.out.println("[B] Processing: " + msg.getMessage());
    doHeavyWork(msg.getMessage());
    msg.ack();
});

// Both workers signal readiness at startup
workerA.setProcessorActive("job-worker", true);
workerB.setProcessorActive("job-worker", true);

// Later: server-1 goes busy — pause without disconnecting
workerA.setProcessorActive("job-worker", false); // all jobs now go to B

// Later: server-1 is free again
workerA.setProcessorActive("job-worker", true);  // jobs distributed across A and B again

// Publish jobs from anywhere
messaging.sendMessage("render-jobs", "job:42");
messaging.sendMessage("render-jobs", "job:43");
// job:42 → A or B, job:43 → the other one
```

> **Note:** `msg.ack()` must always be called when the job is done.  
> If the handler throws, the message is rejected (no requeue) to prevent infinite loops.

---

## Auto-recovery (RabbitMQ)

The connection is configured with:
- `automaticRecoveryEnabled = true`
- `networkRecoveryInterval = 5s`
- `requestedHeartbeat = 30s`

After a reconnection, all registered processors are automatically re-subscribed.  
No manual intervention needed.

---

## Request/Reply pattern

```java
// Server side — listens and replies
messaging.registerMessageProcessor("server-queue", "server.requests", msg -> {
    String reply = process(msg.getMessage());
    try {
        if (msg.getCallbackRoutingKey() != null) {
            messaging.sendMessage(msg.getCallbackRoutingKey(), reply);
        }
        msg.ack();
    } catch (Exception e) { /* handle */ }
});

// Client side — sends and listens for the reply
String myCallbackKey = "client.reply." + UUID.randomUUID();
messaging.registerMessageProcessor("client-reply", myCallbackKey, msg -> {
    System.out.println("Reply: " + msg.getMessage());
    msg.ack();
    messaging.unregisterMessageProcessor("client-reply");
});
messaging.sendMessage("server.requests", myCallbackKey, "my request");
```
