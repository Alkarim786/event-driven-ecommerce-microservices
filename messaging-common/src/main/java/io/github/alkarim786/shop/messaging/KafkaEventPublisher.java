package io.github.alkarim786.shop.messaging;

import org.springframework.kafka.core.KafkaTemplate;

import java.util.concurrent.CompletableFuture;

/** {@link EventPublisher} backed by an idempotent Kafka producer (acks=all, enable.idempotence=true). */
public class KafkaEventPublisher implements EventPublisher {

    private final KafkaTemplate<String, String> template;

    public KafkaEventPublisher(KafkaTemplate<String, String> template) {
        this.template = template;
    }

    @Override
    public CompletableFuture<Void> publish(String topic, String key, String value) {
        return template.send(topic, key, value).thenApply(result -> null);
    }
}
