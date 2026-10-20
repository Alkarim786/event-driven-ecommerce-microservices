package io.github.alkarim786.shop.messaging;

import io.github.alkarim786.shop.events.EventCodec;
import io.github.alkarim786.shop.events.Topics;
import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.jdbc.JdbcTemplateAutoConfiguration;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.boot.autoconfigure.transaction.TransactionAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.util.backoff.FixedBackOff;

import java.time.Clock;
import java.util.ArrayList;
import java.util.List;

/** Wires the outbox, relay, idempotent consumer, topics and dead-letter handling into each service. */
@AutoConfiguration(after = {JdbcTemplateAutoConfiguration.class, TransactionAutoConfiguration.class,
        KafkaAutoConfiguration.class})
@EnableConfigurationProperties(OutboxProperties.class)
public class MessagingAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    Clock clock() {
        return Clock.systemUTC();
    }

    @Bean
    @ConditionalOnMissingBean
    EventCodec eventCodec() {
        return new EventCodec();
    }

    @Bean
    OutboxWriter outboxWriter(JdbcTemplate jdbc, EventCodec codec) {
        return new OutboxWriter(jdbc, codec);
    }

    @Bean
    IdempotentConsumer idempotentConsumer(JdbcTemplate jdbc, TransactionTemplate tx, Clock clock,
                                          @Value("${shop.consumer.max-attempts:10}") int maxAttempts) {
        return new IdempotentConsumer(jdbc, tx, clock, maxAttempts);
    }

    @Bean
    @ConditionalOnMissingBean
    EventPublisher eventPublisher(KafkaTemplate<String, String> template) {
        return new KafkaEventPublisher(template);
    }

    @Bean
    @ConditionalOnProperty(name = "shop.outbox.enabled", havingValue = "true", matchIfMissing = true)
    OutboxRelay outboxRelay(JdbcTemplate jdbc, TransactionTemplate tx, EventPublisher publisher,
                            OutboxProperties props, Clock clock, OutboxWriter writer,
                            ObjectProvider<MeterRegistry> meters) {
        OutboxRelay relay = new OutboxRelay(jdbc, tx, publisher, props, clock);
        writer.onCommit(relay::wakeUp);
        meters.ifAvailable(registry -> {
            Gauge.builder("outbox.pending", relay, OutboxRelay::pendingCount)
                    .description("Outbox rows not yet published").register(registry);
            FunctionCounter.builder("outbox.published", relay, OutboxRelay::publishedTotal)
                    .description("Outbox rows published to Kafka").register(registry);
        });
        return relay;
    }

    /** All topics plus their dead-letter topics, created on startup if missing. */
    @Bean
    KafkaAdmin.NewTopics shopTopics(@Value("${shop.kafka.partitions:3}") int partitions) {
        List<NewTopic> topics = new ArrayList<>();
        for (String t : Topics.ALL) {
            topics.add(TopicBuilder.name(t).partitions(partitions).replicas(1).build());
            topics.add(TopicBuilder.name(Topics.deadLetter(t)).partitions(1).replicas(1).build());
        }
        return new KafkaAdmin.NewTopics(topics.toArray(NewTopic[]::new));
    }

    /**
     * Retries a failing record a few times (transient DB errors, lock conflicts), then publishes it
     * to {@code <topic>.DLT} so one poison message cannot block its partition forever. Malformed
     * JSON is never retried.
     */
    @Bean
    CommonErrorHandler kafkaErrorHandler(KafkaTemplate<String, String> template,
                                         @Value("${shop.consumer.retry-interval-ms:200}") long intervalMs,
                                         @Value("${shop.consumer.retries:4}") long retries) {
        var recoverer = new DeadLetterPublishingRecoverer(template,
                (record, ex) -> new TopicPartition(Topics.deadLetter(record.topic()), 0));
        var handler = new DefaultErrorHandler(recoverer, new FixedBackOff(intervalMs, retries));
        handler.addNotRetryableExceptions(EventCodec.MalformedEventException.class);
        return handler;
    }
}
