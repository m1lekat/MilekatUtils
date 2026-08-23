package fr.milekat.sabot;

import fr.milekat.utils.MileLogger;
import fr.milekat.utils.messaging.MessagingConnection;
import org.bukkit.command.Command;
import org.bukkit.command.CommandExecutor;
import org.bukkit.command.CommandSender;
import org.bukkit.command.TabCompleter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * All subcommands are deliberately flat and unguarded — this plugin only ever runs on a
 * disposable single-purpose test server, never alongside real players or other plugins.
 */
public class SabotCommand implements CommandExecutor, TabCompleter {
    private final SabotPlugin plugin;
    private final MileLogger logger;
    private final @Nullable MessagingConnection messaging;

    public SabotCommand(SabotPlugin plugin, MileLogger logger, @Nullable MessagingConnection messaging) {
        this.plugin = plugin;
        this.logger = logger;
        this.messaging = messaging;
    }

    @Override
    public boolean onCommand(@NotNull CommandSender sender, @NotNull Command command,
                              @NotNull String label, String[] args) {
        if (args.length == 0) {
            sender.sendMessage("/sabot status | messaging <send|topic|task|active|unregister> ...");
            return true;
        }
        switch (args[0].toLowerCase()) {
            case "status":
                sender.sendMessage("messaging: " + (messaging != null ? "loaded (" + messaging.getVendor() + ")" : "disabled"));
                return true;
            case "messaging":
                return handleMessaging(sender, Arrays.copyOfRange(args, 1, args.length));
            default:
                sender.sendMessage("Unknown subcommand: " + args[0]);
                return true;
        }
    }

    private boolean handleMessaging(CommandSender sender, String[] args) {
        if (messaging == null) {
            sender.sendMessage("Messaging is disabled — set messaging.enabled: true in sabot.yml and restart.");
            return true;
        }
        if (args.length == 0) {
            sender.sendMessage("/sabot messaging <send|topic|task|active|unregister> ...");
            return true;
        }
        try {
            switch (args[0].toLowerCase()) {
                case "send": {
                    // /sabot messaging send <routingKey> <message...>
                    String routingKey = args[1];
                    String message = String.join(" ", Arrays.copyOfRange(args, 2, args.length));
                    messaging.sendMessage(routingKey, message);
                    sender.sendMessage("Sent to '" + routingKey + "': " + message);
                    return true;
                }
                case "topic": {
                    // /sabot messaging topic <name> <routingKeyPattern>
                    String name = args[1];
                    String routingKey = args[2];
                    messaging.registerMessageProcessor(name, routingKey, msg -> {
                        logger.info("[topic:" + name + "] " + msg.getRoutingKey() + " -> " + msg.getMessage());
                        try {
                            msg.ack();
                        } catch (Exception e) {
                            logger.warning("Failed to ack message on '" + name + "': " + e.getMessage());
                        }
                    });
                    sender.sendMessage("Registered topic processor '" + name + "' on '" + routingKey + "'");
                    return true;
                }
                case "task": {
                    // /sabot messaging task <name> <queueName>
                    String name = args[1];
                    String queueName = args[2];
                    messaging.registerTaskProcessor(name, queueName, msg -> {
                        logger.info("[task:" + name + "] " + msg.getRoutingKey() + " -> " + msg.getMessage());
                        try {
                            msg.ack();
                        } catch (Exception e) {
                            logger.warning("Failed to ack message on '" + name + "': " + e.getMessage());
                        }
                    });
                    sender.sendMessage("Registered task processor '" + name + "' on queue '" + queueName
                            + "' (inactive — use /sabot messaging active " + name + " true)");
                    return true;
                }
                case "active": {
                    // /sabot messaging active <name> <true|false>
                    String name = args[1];
                    boolean active = Boolean.parseBoolean(args[2]);
                    messaging.setProcessorActive(name, active);
                    sender.sendMessage("Processor '" + name + "' active=" + active);
                    return true;
                }
                case "unregister": {
                    String name = args[1];
                    messaging.unregisterMessageProcessor(name);
                    sender.sendMessage("Unregistered processor '" + name + "'");
                    return true;
                }
                default:
                    sender.sendMessage("Unknown messaging subcommand: " + args[0]);
                    return true;
            }
        } catch (ArrayIndexOutOfBoundsException e) {
            sender.sendMessage("Missing argument for '" + args[0] + "'");
            return true;
        } catch (Exception e) {
            sender.sendMessage("Error: " + e.getMessage());
            logger.warning("Sabot command error: " + e);
            return true;
        }
    }

    @Override
    public @Nullable List<String> onTabComplete(@NotNull CommandSender sender, @NotNull Command command,
                                                 @NotNull String alias, String[] args) {
        if (args.length == 1) {
            return Arrays.asList("status", "messaging");
        }
        if (args.length == 2 && args[0].equalsIgnoreCase("messaging")) {
            return Arrays.asList("send", "topic", "task", "active", "unregister");
        }
        return Collections.emptyList();
    }
}
