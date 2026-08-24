package fr.milekat.sabot;

import fr.milekat.utils.Configs;
import fr.milekat.utils.MileLogger;
import fr.milekat.utils.messaging.adapter.rabbitmq.RabbitMQConnection;
import org.bukkit.command.CommandSender;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Proves {@code sendMessage} can reach a task queue registered by a connection it has never
 * seen — the actual ZeroInfra scenario (proxy publishes, private consumes, two different pods,
 * two different {@code RabbitMQConnection} instances with no shared state). Registering the
 * consumer and sending from the <em>same</em> connection (as {@code /sabot messaging task} +
 * {@code send} would) can't catch this: the old local-registration check would pass trivially
 * either way.
 *
 * <p>Builds two independent {@code RabbitMQConnection} instances directly (bypassing the
 * shared plugin connection) specifically so neither one's local state can leak into the other.
 */
public class CrossProcessTaskTest {
    private static final String QUEUE_NAME = "sabot.crossprocess.test";
    private static final String PROCESSOR_NAME = "sabot-crossprocess-consumer";
    private static final long TIMEOUT_SECONDS = 10;

    private CrossProcessTaskTest() {
    }

    public static void run(Configs config, MileLogger logger, CommandSender sender) {
        sender.sendMessage("Cross-process test: opening two independent connections...");

        RabbitMQConnection consumerConn;
        RabbitMQConnection producerConn;
        try {
            consumerConn = new RabbitMQConnection(config, logger);
            producerConn = new RabbitMQConnection(config, logger);
        } catch (Exception e) {
            sender.sendMessage("Failed to open connections: " + e.getMessage());
            return;
        }

        CountDownLatch received = new CountDownLatch(1);
        AtomicReference<String> receivedBody = new AtomicReference<>();

        consumerConn.registerTaskProcessor(PROCESSOR_NAME, QUEUE_NAME, msg -> {
            receivedBody.set(msg.getMessage());
            try {
                msg.ack();
            } catch (Exception e) {
                logger.warning("Cross-process test: failed to ack: " + e.getMessage());
            }
            received.countDown();
        });
        consumerConn.setProcessorActive(PROCESSOR_NAME, true);

        String payload = "cross-process-" + System.currentTimeMillis();
        try {
            // producerConn has registered nothing locally — this call has no way to know
            // QUEUE_NAME is a task queue except by asking the broker itself.
            producerConn.sendMessage(QUEUE_NAME, payload);
        } catch (Exception e) {
            sender.sendMessage("Send failed: " + e.getMessage());
            consumerConn.close();
            producerConn.close();
            return;
        }

        sender.sendMessage("Sent '" + payload + "' from the producer-only connection. Waiting...");

        Thread waiter = new Thread(() -> {
            boolean arrived;
            try {
                arrived = received.await(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                arrived = false;
            }

            String result;
            if (!arrived) {
                result = "Cross-process test: TIMEOUT after " + TIMEOUT_SECONDS +
                        "s — message never arrived, broker-side detection failed";
            } else if (payload.equals(receivedBody.get())) {
                result = "Cross-process test: PASSED — received '" + receivedBody.get() +
                        "' on the consumer connection, sent by a connection that never registered it";
            } else {
                result = "Cross-process test: MISMATCH — expected '" + payload + "', got '" +
                        receivedBody.get() + "'";
            }

            logger.info(result);
            sender.sendMessage(result);
            consumerConn.close();
            producerConn.close();
        }, "sabot-crossprocess-test");
        waiter.setDaemon(true);
        waiter.start();
    }
}
