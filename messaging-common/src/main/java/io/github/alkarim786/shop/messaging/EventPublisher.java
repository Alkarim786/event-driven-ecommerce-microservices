package io.github.alkarim786.shop.messaging;

import java.util.concurrent.CompletableFuture;

/**
 * Sends one already-serialised event to the broker. The future completes when the broker has
 * acknowledged the write (acks=all for Kafka). Implementations: {@link KafkaEventPublisher}, and
 * in-memory fakes in tests.
 */
public interface EventPublisher {
    CompletableFuture<Void> publish(String topic, String key, String value);
}
