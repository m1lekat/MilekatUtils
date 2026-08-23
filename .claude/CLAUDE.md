# Local-server testing (SNAPSHOT features)

Full detail/history/bugs found: `docs/local-server-testing.md` (only if the below isn't enough).

## Build

```
./gradlew :sabot-plugin:shadowJar
```
Jar: `sabot-plugin/build/libs/MilekatSabot-*.jar`. Toggle subsystems before building in
`sabot-plugin/src/main/resources/sabot.yml` (`messaging.enabled` / `storage.enabled`) —
enable only what the current change touches.

## Target (dedicated, disposable — no confirmation needed to redeploy/restart/overwrite)

- Pterodactyl panel `milekat` (creds: ClaudeSmart `secrets/pterodactyl/milekat.env`)
- Server `milekat-sabot`, identifier `98930545`

## Deploy loop

```
# 1. upload jar
curl -X POST -H "Authorization: Bearer $PTERODACTYL_CLIENT_TOKEN" --data-binary @<jar> \
  "$PTERODACTYL_PANEL_URL/api/client/servers/98930545/files/write?file=%2Fplugins%2F<jarname>"

# 2. write REAL sabot.yml (real creds, e.g. ClaudeSmart secrets/rabbitmq/local.env — never the
#    committed template, never git-committed). RabbitMQ host must be 172.18.0.1, not localhost
#    (Pterodactyl containers are on the pterodactyl_nw bridge; localhost = the container itself).
curl -X POST -H "Authorization: Bearer $PTERODACTYL_CLIENT_TOKEN" --data-binary @<realconfig.yml> \
  "$PTERODACTYL_PANEL_URL/api/client/servers/98930545/files/write?file=%2Fplugins%2FMilekatSabot%2Fsabot.yml"

# 3. restart
curl -X POST -H "Authorization: Bearer $PTERODACTYL_CLIENT_TOKEN" -H "Content-Type: application/json" \
  -d '{"signal":"restart"}' "$PTERODACTYL_PANEL_URL/api/client/servers/98930545/power"

# 4. watch / drive
python3 .claude/skills/pterodactyl/scripts/console.py 98930545 [--command "sabot messaging ..."]
```
