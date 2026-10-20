package io.github.alkarim786.shop.notification;

import io.github.alkarim786.shop.events.EventCodec;
import io.github.alkarim786.shop.events.Topics;
import io.github.alkarim786.shop.messaging.IdempotentConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.List;

/** Kafka listener and read-only REST endpoint for notification-service. */
final class NotificationWeb {
    private NotificationWeb() {}

    record NotificationView(String orderId, String customerId, String channel, String eventType,
                            String message, Instant createdAt) {}

    @RestController
    static class NotificationController {
        private final JdbcTemplate jdbc;

        NotificationController(JdbcTemplate jdbc) {
            this.jdbc = jdbc;
        }

        @GetMapping("/notifications")
        List<NotificationView> byOrder(@RequestParam String orderId) {
            return jdbc.query("SELECT * FROM notifications WHERE order_id = ? ORDER BY id",
                    (rs, i) -> new NotificationView(rs.getString("order_id"), rs.getString("customer_id"),
                            rs.getString("channel"), rs.getString("event_type"), rs.getString("message"),
                            rs.getTimestamp("created_at").toInstant()), orderId);
        }
    }

    @Component
    static class NotificationEventsListener {
        private final EventCodec codec;
        private final IdempotentConsumer idempotent;
        private final NotificationHandler handler;

        NotificationEventsListener(EventCodec codec, IdempotentConsumer idempotent, NotificationHandler handler) {
            this.codec = codec;
            this.idempotent = idempotent;
            this.handler = handler;
        }

        @KafkaListener(topics = Topics.ORDERS, groupId = NotificationHandler.CONSUMER_GROUP)
        void onEvent(ConsumerRecord<String, String> record) {
            idempotent.process(NotificationHandler.CONSUMER_GROUP, codec.parse(record.value()), handler::handle);
        }
    }
}
