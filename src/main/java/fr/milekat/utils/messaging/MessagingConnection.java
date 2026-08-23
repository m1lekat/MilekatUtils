package fr.milekat.utils.messaging;

import fr.milekat.utils.messaging.exceptions.MessagingLoadException;
import fr.milekat.utils.messaging.exceptions.MessagingSendException;
import org.jetbrains.annotations.NotNull;

import java.util.UUID;
import java.util.function.Consumer;

@SuppressWarnings("unused")
public interface MessagingConnection {
    /**
     * Initializes the messaging connection.
     *
     * @throws MessagingLoadException if the connection could not be established
     */
    void initConnection() throws MessagingLoadException;

    /**
     * Checks if the messaging connection is ready.
     *
     * @return true if the connection is ready, false otherwise
     * @throws MessagingLoadException if there was an error checking the connection status
     */
    boolean connectionReady() throws MessagingLoadException;

    /**
     * Closes the messaging connection. It also stops any message processing.
     *
     * @throws MessagingLoadException if there was an error closing the connection
     */
    void close();

    /**
     * Gets the messaging vendor associated with this connection.
     *
     * @return the MessagingVendor enum representing the vendor
     */
    MessagingVendor getVendor();

    /**
     * Sends a message to the specified target.
     * <p>This method is a convenience method that allows
     * sending a message without specifying a callback routing key.</p>
     * @param targetRoutingKey the target identifier (Mostly a routing key or channel name)
     * @param message the message to send
     * @throws MessagingSendException if there was an error sending the message
     */
    default void sendMessage(String targetRoutingKey, String message) throws MessagingSendException {
        sendMessage(targetRoutingKey, null, message);
    }

    /**
     * Send a message with JSON format including tag and callback routing key
     *
     * @param targetRoutingKey The routing key to send the message to
     * @param senderCallBackKey The routing key for callback/reply messages
     * @param message The actual message content
     * @throws MessagingSendException if sending fails
     */
    void sendMessage(String targetRoutingKey, String senderCallBackKey, String message) throws MessagingSendException;

    /**
     * Registers a message processor for a specific routing key.
     * This processor will handle incoming messages that match the routing key.
     *
     * @param routingKey the routing key to listen for messages
     * @param messageHandler consumer that processes the received messages
     * @return a {@link String} representing the id of the message processor (To be used for unregistration)
     * @throws MessagingLoadException if there was an error registering the message processor
     */
    default String registerMessageProcessor(@NotNull String routingKey,
                                            @NotNull Consumer<ReceivedMessage> messageHandler)
            throws MessagingLoadException {
        String processorName = UUID.randomUUID().toString();
        registerMessageProcessor(processorName, routingKey, messageHandler);
        return processorName;
    }
    /**
     * Registers a message processor for a specific routing key.
     * This processor will handle incoming messages that match the routing key.
     *
     * @param processorName the name of the message processor (used for identification and management)
     * @param routingKey the routing key to listen for messages
     * @param messageHandler consumer that processes the received messages
     * @throws MessagingLoadException if there was an error registering the message processor
     */
    void registerMessageProcessor(@NotNull String processorName, @NotNull String routingKey,
                                  @NotNull Consumer<ReceivedMessage> messageHandler) throws MessagingLoadException;

    /**
     * Registers a task queue processor (competing consumers mode).
     * Multiple workers can register with the same {@code queueName}; each message is delivered to
     * exactly one worker. The processor starts <strong>inactive</strong> — call
     * {@link #setProcessorActive(String, boolean)} when the worker is ready to accept tasks.
     *
     * @param queueName the shared queue name (same across all competing workers for this task type)
     * @param messageHandler consumer that processes the received message
     * @return a UUID string identifying this processor instance (for management calls)
     * @throws MessagingLoadException if registration fails
     */
    default String registerTaskProcessor(@NotNull String queueName,
                                         @NotNull Consumer<ReceivedMessage> messageHandler)
            throws MessagingLoadException {
        String processorName = UUID.randomUUID().toString();
        registerTaskProcessor(processorName, queueName, messageHandler);
        return processorName;
    }

    /**
     * Registers a task queue processor (competing consumers mode).
     * Multiple workers can register with the same {@code queueName}; each message is delivered to
     * exactly one worker. The processor starts <strong>inactive</strong> — call
     * {@link #setProcessorActive(String, boolean)} when the worker is ready to accept tasks.
     *
     * @param processorName unique name for this processor instance
     * @param queueName the shared queue name (same across all competing workers for this task type)
     * @param messageHandler consumer that processes the received message
     * @throws MessagingLoadException if a processor with the same name already exists or setup fails
     */
    void registerTaskProcessor(@NotNull String processorName, @NotNull String queueName,
                                @NotNull Consumer<ReceivedMessage> messageHandler) throws MessagingLoadException;

    /**
     * Activates or deactivates a task processor.
     *
     * <p>When <strong>inactive</strong>, the worker holds no AMQP consumer subscription —
     * no messages are pushed to it and the queue is not consumed.
     * When <strong>active</strong>, the worker registers an AMQP consumer and starts
     * receiving messages from the shared queue.
     *
     * <p>This allows a worker to signal readiness without connecting or disconnecting:
     * call {@code setProcessorActive("worker", true)} when ready to accept a task,
     * and {@code setProcessorActive("worker", false)} when busy.
     *
     * <p>Only applicable to processors registered via {@link #registerTaskProcessor}.
     *
     * @param processorName the name of the task processor
     * @param active {@code true} to start consuming, {@code false} to stop consuming
     * @throws MessagingLoadException if the processor is unknown, is not a task processor,
     *                               or activation fails
     */
    void setProcessorActive(@NotNull String processorName, boolean active) throws MessagingLoadException;

    /**
     * Unregisters a message processor by its name.
     * This stops the processor from receiving any further messages.
     *
     * @param processorName the name of the message processor to unregister
     */
    void unregisterMessageProcessor(String processorName);
}
