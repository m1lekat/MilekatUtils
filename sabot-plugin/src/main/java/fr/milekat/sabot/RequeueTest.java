package fr.milekat.sabot;

import fr.milekat.utils.MileLogger;
import fr.milekat.utils.messaging.MessagingConnection;
import org.bukkit.command.CommandSender;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Registers a task processor that rejects the first delivery of each message with
 * {@code requeue = true}, then acks the redelivery — proves the broker actually honors the
 * requeue flag on {@link fr.milekat.utils.messaging.ReceivedMessage#reject(boolean)} instead
 * of just not throwing.
 */
public class RequeueTest {
    private static final String PROCESSOR_NAME = "sabot-requeue-test";
    private static final String QUEUE_NAME = "sabot.requeue.test";

    private final Set<String> seenOnce = ConcurrentHashMap.newKeySet();

    private RequeueTest() {
    }

    public static void run(MileLogger logger, MessagingConnection messaging, CommandSender sender) {
        RequeueTest test = new RequeueTest();
        messaging.registerTaskProcessor(PROCESSOR_NAME, QUEUE_NAME, msg -> {
            String body = msg.getMessage();
            try {
                if (test.seenOnce.add(body)) {
                    logger.info("Requeue test: first delivery of '" + body + "' — rejecting with requeue=true");
                    msg.reject(true);
                } else {
                    logger.info("Requeue test: redelivery of '" + body + "' — acking, requeue confirmed working");
                    msg.ack();
                }
            } catch (Exception e) {
                logger.warning("Requeue test: failed to ack/reject: " + e.getMessage());
            }
        });
        messaging.setProcessorActive(PROCESSOR_NAME, true);
        sender.sendMessage("Requeue test processor active on queue '" + QUEUE_NAME +
                "'. Send a message with: /sabot messaging send " + QUEUE_NAME + " <text>");
    }

    public static void cleanup(MessagingConnection messaging, CommandSender sender) {
        messaging.unregisterMessageProcessor(PROCESSOR_NAME);
        sender.sendMessage("Requeue test processor unregistered.");
    }
}
