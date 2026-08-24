package fr.milekat.sabot;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.impl.AMQConnection;
import com.rabbitmq.client.impl.FrameHandler;
import com.rabbitmq.client.impl.recovery.AutorecoveringConnection;
import fr.milekat.utils.MileLogger;
import fr.milekat.utils.messaging.MessagingConnection;
import fr.milekat.utils.messaging.adapter.rabbitmq.RabbitMQConnection;
import org.bukkit.command.CommandSender;
import org.bukkit.plugin.java.JavaPlugin;
import org.bukkit.scheduler.BukkitRunnable;

import java.lang.reflect.Field;

/**
 * Forces an unexpected mid-use disconnection on the live RabbitMQ connection and reports
 * whether/how fast the client's automatic recovery brings it back.
 *
 * <p>Deliberately kept entirely inside this test-harness plugin instead of adding a
 * test-only method to MilekatUtils: this class reaches into {@link RabbitMQConnection}'s
 * private {@code connection} field via reflection, then follows amqp-client's own
 * (public, if undocumented) internal API — {@code AutorecoveringConnection.getDelegate()},
 * {@code AMQConnection.getFrameHandler()}, {@code FrameHandler.close()} — to sever the
 * socket without going through the AMQP close handshake. The production library carries
 * none of this; sabot is where functional test tooling like this belongs. Fragile across
 * amqp-client/MilekatUtils versions by design — this is throwaway test tooling, not a
 * supported integration point.
 *
 * <p>This exercises a different failure mode than the constructor's initial-connect retry
 * loop: a connection that already succeeded once and then drops mid-use, which is exactly
 * what {@code setAutomaticRecoveryEnabled(true)} — not the retry loop — is responsible for.
 */
public class DisconnectTest {
    private static final long POLL_INTERVAL_TICKS = 10L; // 0.5s
    private static final long TIMEOUT_TICKS = 20L * 30L; // 30s

    private DisconnectTest() {
    }

    public static void run(JavaPlugin plugin, MileLogger logger, MessagingConnection messaging,
                            CommandSender sender) {
        if (!(messaging instanceof RabbitMQConnection rabbit)) {
            sender.sendMessage("Drop test only supports the RabbitMQ adapter (current: "
                    + (messaging == null ? "none" : messaging.getVendor()) + ")");
            return;
        }
        if (!messaging.connectionReady()) {
            sender.sendMessage("Connection isn't ready before the test even starts — fix that first.");
            return;
        }

        sender.sendMessage("Connection is ready. Forcing an unexpected disconnect...");
        if (!forceDisconnect(rabbit, logger, sender)) {
            return;
        }

        long start = System.currentTimeMillis();
        new BukkitRunnable() {
            long ticksWaited = 0;
            boolean sawDrop = false;

            @Override
            public void run() {
                boolean ready = messaging.connectionReady();

                if (!sawDrop) {
                    if (!ready) {
                        sawDrop = true;
                        logger.info("Drop test: connection is down, waiting for automatic recovery...");
                    }
                } else if (ready) {
                    long elapsedMs = System.currentTimeMillis() - start;
                    String msg = "Drop test: reconnected after " + elapsedMs + "ms";
                    logger.info(msg);
                    sender.sendMessage(msg);
                    cancel();
                    return;
                }

                ticksWaited += POLL_INTERVAL_TICKS;
                if (ticksWaited >= TIMEOUT_TICKS) {
                    String msg = sawDrop
                            ? "Drop test: still not reconnected after " + (TIMEOUT_TICKS / 20) +
                                "s — automatic recovery may not be working"
                            : "Drop test: connection never even registered as down — the socket "
                                + "close may not have reached the client";
                    logger.warning(msg);
                    sender.sendMessage(msg);
                    cancel();
                }
            }
        }.runTaskTimer(plugin, POLL_INTERVAL_TICKS, POLL_INTERVAL_TICKS);
    }

    /**
     * Reaches past {@link RabbitMQConnection}'s public surface to sever the live socket.
     * {@link Connection#close()} / {@link Connection#abort()} are application-initiated and
     * automatic recovery deliberately ignores them — only a socket-level failure (this) is
     * indistinguishable from a real network drop or broker crash.
     */
    private static boolean forceDisconnect(RabbitMQConnection rabbit, MileLogger logger, CommandSender sender) {
        try {
            Field connectionField = RabbitMQConnection.class.getDeclaredField("connection");
            connectionField.setAccessible(true);
            Connection connection = (Connection) connectionField.get(rabbit);

            if (!(connection instanceof AutorecoveringConnection autorecovering)) {
                sender.sendMessage("Connection isn't an AutorecoveringConnection — is automatic recovery enabled?");
                return false;
            }

            AMQConnection delegate = autorecovering.getDelegate();
            FrameHandler frameHandler = delegate.getFrameHandler();
            logger.warning("Drop test: severing the socket directly (test only)");
            frameHandler.close();
            return true;
        } catch (ReflectiveOperationException | ClassCastException e) {
            sender.sendMessage("Failed to reach the connection internals: " + e);
            return false;
        }
    }
}
