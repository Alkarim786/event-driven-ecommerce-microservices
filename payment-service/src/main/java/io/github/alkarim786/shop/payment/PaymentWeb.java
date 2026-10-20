package io.github.alkarim786.shop.payment;

import io.github.alkarim786.shop.events.EventCodec;
import io.github.alkarim786.shop.events.Topics;
import io.github.alkarim786.shop.messaging.IdempotentConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.UUID;

/** Kafka listener and read-only REST endpoint for payment-service. */
final class PaymentWeb {
    private PaymentWeb() {}

    record PaymentView(UUID paymentId, String orderId, String customerId, long amountCents,
                       String status, String failureReason, Instant createdAt) {}

    @RestController
    static class PaymentController {
        private final JdbcTemplate jdbc;

        PaymentController(JdbcTemplate jdbc) {
            this.jdbc = jdbc;
        }

        @GetMapping("/payments/{orderId}")
        ResponseEntity<PaymentView> byOrder(@PathVariable String orderId) {
            return ResponseEntity.of(jdbc.query("SELECT * FROM payments WHERE order_id = ?",
                    (rs, i) -> new PaymentView(rs.getObject("id", UUID.class), rs.getString("order_id"),
                            rs.getString("customer_id"), rs.getLong("amount_cents"), rs.getString("status"),
                            rs.getString("failure_reason"), rs.getTimestamp("created_at").toInstant()),
                    orderId).stream().findFirst());
        }
    }

    @Component
    static class PaymentEventsListener {
        private final EventCodec codec;
        private final IdempotentConsumer idempotent;
        private final PaymentHandler handler;

        PaymentEventsListener(EventCodec codec, IdempotentConsumer idempotent, PaymentHandler handler) {
            this.codec = codec;
            this.idempotent = idempotent;
            this.handler = handler;
        }

        @KafkaListener(topics = Topics.INVENTORY, groupId = PaymentHandler.CONSUMER_GROUP)
        void onEvent(ConsumerRecord<String, String> record) {
            idempotent.process(PaymentHandler.CONSUMER_GROUP, codec.parse(record.value()), handler::handle);
        }
    }
}
