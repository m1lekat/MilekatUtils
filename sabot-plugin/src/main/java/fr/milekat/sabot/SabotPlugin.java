package fr.milekat.sabot;

import fr.milekat.utils.Configs;
import fr.milekat.utils.MileLogger;
import fr.milekat.utils.messaging.MessagingConnection;
import fr.milekat.utils.messaging.MessagingLoader;
import org.bukkit.plugin.java.JavaPlugin;

import java.io.File;

/**
 * Throwaway harness: loads only the MilekatUtils subsystems enabled in sabot.yml, so a
 * SNAPSHOT feature can be exercised on a real server without needing every backend
 * (e.g. Elasticsearch) available. See docs/local-server-testing.md in the main project.
 */
public class SabotPlugin extends JavaPlugin {
    private MileLogger logger;
    private MessagingConnection messaging;
    private Configs config;

    @Override
    public void onEnable() {
        logger = new MileLogger(getLogger());
        saveResource("sabot.yml", false);
        config = new Configs(new File(getDataFolder(), "sabot.yml"));

        if (config.getBoolean("messaging.enabled", false)) {
            try {
                messaging = new MessagingLoader(config, logger).getLoadedMessaging();
                logger.info("Messaging subsystem loaded (" + messaging.getVendor() + ")");
            } catch (Exception e) {
                logger.warning("Messaging enabled in sabot.yml but failed to load: " + e.getMessage());
            }
        } else {
            logger.info("Messaging subsystem disabled in sabot.yml — skipped");
        }

        if (config.getBoolean("storage.enabled", false)) {
            logger.warning("Storage subsystem toggle is on but not wired up in this sabot build yet");
        } else {
            logger.info("Storage subsystem disabled in sabot.yml — skipped");
        }

        SabotCommand command = new SabotCommand(this, logger, messaging, config);
        getCommand("sabot").setExecutor(command);
        getCommand("sabot").setTabCompleter(command);
    }

    @Override
    public void onDisable() {
        if (messaging != null) {
            messaging.close();
        }
    }
}
