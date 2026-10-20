package io.github.alkarim786.shop.notification;

import io.github.alkarim786.shop.events.EventCodec;
import io.github.alkarim786.shop.events.EventEnvelope;
import io.github.alkarim786.shop.events.EventTypes;
import io.github.alkarim786.shop.events.Payloads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.sql.Timestamp;
import java.time.Clock;
import java.util.Locale;

/**
 * Sends the customer one message per terminal order event. The notification row is written in
 * the idempotent-consumer transaction, so a redelivered event never sends a second email.
 * (A real sender would be called after commit from an outbox of its own; here sending is a log line.)
 */
@Component
public class NotificationHandler {

    public static final String CONSUMER_GROUP = "notification-service";
    private static final Logger log = LoggerFactory.getLogger(NotificationHandler.class);

    private final JdbcTemplate jdbc;
    private final EventCodec codec;
    private final Clock clock;

    public NotificationHandler(JdbcTemplate jdbc, EventCodec codec, Clock clock) {
        this.jdbc = jdbc;
        this.codec = codec;
        this.clock = clock;
    }

    public void handle(EventEnvelope event) {
        switch (event.type()) {
            case EventTypes.ORDER_CONFIRMED -> {
                var p = codec.payload(event, Payloads.OrderConfirmed.class);
                notify(p.orderId(), p.customerId(), event.type(), String.format(Locale.ROOT,
                        "Your order %s is confirmed. Total charged: %.2f", p.orderId(), p.totalCents() / 100.0));
            }
            case EventTypes.ORDER_CANCELLED -> {
                var p = codec.payload(event, Payloads.OrderCancelled.class);
                notify(p.orderId(), p.customerId(), event.type(),
                        "Your order " + p.orderId() + " was cancelled: " + p.reason() + ". You have not been charged.");
            }
            default -> log.debug("notification-service ignores {}", event.type());
        }
    }

    private void notify(String orderId, String customerId, String type, String message) {
        jdbc.update("""
                INSERT INTO notifications (order_id, customer_id, channel, event_type, message, created_at)
                VALUES (?, ?, 'EMAIL', ?, ?, ?)""", orderId, customerId, type, message, Timestamp.from(clock.instant()));
        log.info("EMAIL to {}: {}", customerId, message);
    }
}
