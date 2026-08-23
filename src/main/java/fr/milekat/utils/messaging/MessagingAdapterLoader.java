package fr.milekat.utils.messaging;

import fr.milekat.utils.Configs;
import fr.milekat.utils.MileLogger;
import org.jetbrains.annotations.NotNull;

import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

/**
 * Utility class for dynamically loading messaging adapter implementations.
 *
 * <p>Automatically discovers and instantiates available messaging vendor adapters
 * based on classpath availability. Only creates connections for vendors whose
 * driver classes are present in the runtime environment.
 *
 * @author MileKat
 * @since 1.6
 */
public class MessagingAdapterLoader {

    /**
     * Loads all available messaging adapters based on classpath detection.
     *
     * <p>Iterates through all MessagingVendor values and attempts to create
     * connections only for vendors whose driver classes are available.
     * Failed instantiations are silently ignored to allow partial adapter loading.
     *
     * @param config configuration object containing connection parameters
     * @param logger logger instance for debugging and monitoring
     * @return map of adapter names to their MessagingConnection instances
     */
    public static @NotNull Map<String, MessagingConnection>
    loadAdapters(@NotNull Configs config, @NotNull MileLogger logger) {
        // Create a map to hold the messaging connection instances
        Map<String, MessagingConnection> messagingConnections = new HashMap<>();

        // Iterate over all MessagingVendor values and create a new MessagingConnection for each one
        for (MessagingVendor vendor : MessagingVendor.values()) {
            // Check if the messaging adapter is already in the map, if so, skip it
            if (messagingConnections.containsKey(vendor.getMessagingAdapter())) {
                continue;
            }

            try {
                // Check if the driver class is available
                Class.forName(vendor.getDriverClass());

                // If yes, create a new MessagingConnection instance and add it to the map
                messagingConnections.put(vendor.getMessagingAdapter(),
                        (MessagingConnection) Class.forName(vendor.getAdapterConnectionClass())
                                .getDeclaredConstructor(Configs.class, MileLogger.class)
                                .newInstance(config, logger));
            } catch (ClassNotFoundException ignored) {
                // Driver not on classpath — adapter intentionally skipped
            } catch (NoSuchMethodException | InvocationTargetException |
                     InstantiationException | IllegalAccessException exception) {
                logger.warn("Failed to load messaging adapter for " + vendor.name() + ": " + exception.getMessage());
            }
        }
        return messagingConnections;
    }
}