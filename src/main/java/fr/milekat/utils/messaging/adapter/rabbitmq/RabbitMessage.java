package fr.milekat.utils.messaging.adapter.rabbitmq;

import com.rabbitmq.client.Channel;
import fr.milekat.utils.messaging.ReceivedMessage;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;

public class RabbitMessage implements ReceivedMessage {
    private final Channel channel;
    private final long deliveryTag; // Delivery tag for acknowledgment
    private final String routingKey;
    private final String senderRoutingKey;
    private final String message;
    private boolean acknowledged = false;

    public RabbitMessage(@NotNull Channel channel, long deliveryTag,
                         @NotNull String routingKey, @Nullable String senderRoutingKey,
                         @NotNull String message) {
        this.channel = channel;
        this.deliveryTag = deliveryTag;
        this.routingKey = routingKey;
        this.senderRoutingKey = senderRoutingKey; // Optional, can be null
        this.message = message;
    }

    @Override
    public String getRoutingKey() {
        return routingKey;
    }

    @Override
    public @Nullable String getCallbackRoutingKey() {
        return senderRoutingKey; // Can be null if not set
    }

    @Override
    public String getMessage() {
        return message;
    }

    @Override
    public void ack() throws IOException {
        if (!acknowledged) {
            channel.basicAck(deliveryTag, false);
            acknowledged = true;
        }
    }

    @Override
    public void reject(boolean requeue) throws IOException {
        if (!acknowledged) {
            channel.basicNack(deliveryTag, false, requeue);
            acknowledged = true;
        }
    }

    @Override
    public boolean isAcknowledged() {
        return acknowledged;
    }
}