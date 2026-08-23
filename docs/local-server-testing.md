# Testing a SNAPSHOT build against a real Minecraft server

This documents how to validate a MilekatUtils feature that only exists as a SNAPSHOT (e.g. the
task-queue messaging mode on `1.10.0-SNAPSHOT`) against a real, running Minecraft server instead
of relying on JUnit alone — and a real bug this approach caught on the first try.

## 1. This repo didn't build on this box (fixed)

This host only has JDK 25 installed. Gradle 8.9 can't even parse `build.gradle` under JDK 25
(`Unsupported class file major version 69`), and the old shadow plugin
(`com.github.johnrengelman.shadow:8.1.1`) breaks under Gradle 9 (`MissingPropertyException: 'mode'`
in `ShadowCopyAction`). Fixed here (uncommitted, review before merging):

- `gradle/wrapper/gradle-wrapper.properties`: `8.9` → `9.7.0`
- `build.gradle`: shadow plugin → `com.gradleup.shadow:9.6.1` (same version ZeroInfra already
  pins, so both repos agree on one known-good combination under JDK 25 / Gradle 9)

## 2. The "sabot" plugin — a throwaway harness, not a ZeroInfra change

Earlier draft of this doc proposed patching ZeroInfra's build (mavenLocal + shade override on the
Paper library-loader path) to get a SNAPSHOT running on a real server. That's unnecessary
complexity for what is fundamentally a "does this feature work" question. Instead: a new Gradle
subproject in **this** repo, `sabot-plugin/`, self-contained and disposable:

- `implementation project(':')` — always the current worktree source, never a published or
  `mavenLocal`-installed version. Editing lib code and rebuilding the plugin picks it up
  immediately.
- Shades `milekat-utils` + `amqp-client` straight into the plugin jar
  (`com.gradleup.shadow`), so it never touches Paper's library loader (which only resolves Maven
  Central) or any external package registry. This sidesteps blocker #2 from the previous version
  of this study entirely — no ZeroInfra involvement needed.
- `sabot.yml` (in `sabot-plugin/src/main/resources/`) is a **template only**, feature-toggled:
  ```yaml
  messaging:
    enabled: false   # flip per test — no need to stand up every backend at once
    type: rabbitmq
    rabbitmq: { ... }
  storage:
    enabled: false
  ```
  `SabotPlugin.onEnable()` only calls `MessagingLoader`/`StorageLoader` for the subsystems
  actually enabled. Right now only `messaging` is wired to a command surface — no Elasticsearch
  available in this environment, so `storage.enabled` stays `false` and is a no-op stub for now.
- `/sabot messaging <send|topic|task|active|unregister> ...` (`SabotCommand`) exercises
  `MessagingConnection` directly from the console — no player needed.

### Deployment target

A **dedicated** Pterodactyl server was created for this rather than reusing `lobby` /
`SlimeWorld` / `zero-ship-1`, so a throwaway test plugin never risks an existing instance:

- Panel `milekat`, server `milekat-sabot` (identifier `98930545`), node `node-main`.
- Egg **Paper**, `MINECRAFT_VERSION=26.2`, `ghcr.io/pterodactyl/yolks:java_25`, port `5000`
  (only free allocation on the node at the time), 1024 MB RAM / 2 GB disk.
- Jar built with `./gradlew :sabot-plugin:shadowJar`, uploaded via the Client API files-write
  endpoint to `/plugins/`. Real config (with real broker credentials) is written the same way,
  directly to `/plugins/MilekatSabot/sabot.yml` on the server — **never** through the committed
  template, so no credential ever touches this git history.

### RabbitMQ: dedicated exchange, and a real container-networking gotcha

Per request, this uses the **local** broker (`secrets/rabbitmq/local.env`) with a **dedicated**
exchange name (`milekat.sabot.exchange`) rather than the shared `milekat.exchange` — isolated from
anything else on that broker, auto-created by the library itself on first use (no manual RabbitMQ
setup needed).

`local.env` has `RABBITMQ_HOST=localhost`, which is correct for a process on the host but wrong
for anything running inside a Pterodactyl-managed container: `localhost` there resolves to the
*container*, not the host. RabbitMQ on this box is a host-level systemd service listening on
`*:5672` (all interfaces), so it's reachable from the `pterodactyl_nw` Docker network via that
network's gateway address instead — `172.18.0.1` here (`docker network inspect pterodactyl_nw`).
**Any Pterodactyl-hosted plugin that needs to reach a host-level service must use the bridge
gateway IP, not `localhost`.**

## 3. Bug found: task-queue messages sent via `sendMessage()` never reach the queue

This is the actual point of standing up a live harness instead of trusting JUnit alone, and it
found something real on the first end-to-end pass.

**Reproduction**, live on `milekat-sabot`:
```
> sabot messaging task worker1 test-jobs
[MilekatSabot] Registered task processor 'worker1' for queue: test-jobs (inactive)
> sabot messaging send test-jobs job-while-inactive
Error: Error while sending message to RabbitMQ: channel error; protocol method:
  #method<channel.close>(reply-code=404, reply-text=NOT_FOUND - no exchange
  'milekat.sabot.exchange' in vhost '/', ...)
```

**Root cause**, in `RabbitMQConnection`:

- `registerTaskProcessor()` (line ~691) declares the queue directly —
  `taskChannel.queueDeclare(queueName, true, false, false, null)` — and never binds it to any
  exchange. Every RabbitMQ queue is implicitly reachable through the broker's unnamed **default
  exchange** using the queue name as routing key; that's the only path in.
- `sendMessage()` (line ~463), however, **always** publishes to the configured **custom** exchange
  — `channel.basicPublish(rabbitMQConfig.getName(), targetRoutingKey, ...)` — regardless of
  whether the target is a topic subscription or a task queue.
- The custom exchange is only ever declared as a side effect of registering a **topic** processor
  (`createConsumer`, line 580). A deployment using task-queue mode exclusively (exactly this
  sabot's config) never declares it at all — hence the 404 above.
- Even after the exchange exists (e.g. once some topic processor has registered), the task queue
  is still never *bound* to it, so a published message would simply be dropped, unrouted, with no
  error — silent message loss instead of a 404.

Net effect: **`sendMessage()` cannot deliver to a task queue under any circumstance**, which
directly contradicts the worked example in `docs/messaging/messaging.md` ("Task queue — n workers
example": `messaging.sendMessage("render-jobs", "job:42")`). This is exactly the scale-to-zero
mechanism ZeroInfra's `docs/plugins.md` §"Ce qui manque" is waiting on — as written, it would not
have worked there either.

**Not fixed here** — flagging for a decision before touching `RabbitMQConnection` further. Likely
fix: have `sendMessage()` detect a registered task-queue target (by name, from
`registeredProcessors`) and publish to the default exchange (`""`, routing key = queue name)
in that case instead of the custom exchange.

## 4. Summary

| # | Item | Status |
|---|---|---|
| 1 | Gradle wrapper 8.9 → 9.7.0, shadow plugin → `com.gradleup.shadow:9.6.1` | Done, verified |
| 2 | `sabot-plugin` harness (config-toggleable, shaded, deployed) | Done, running on `milekat-sabot` |
| 3 | Dedicated Pterodactyl test server + dedicated RabbitMQ exchange | Done |
| 4 | `sendMessage()` can't deliver to a task queue (see §3) | **Found, not fixed** — needs a decision |
