package io.github.alkarim786.shop.payment;

import io.github.alkarim786.shop.events.EventCodec;
import io.github.alkarim786.shop.events.EventEnvelope;
import io.github.alkarim786.shop.events.EventTypes;
import io.github.alkarim786.shop.events.Payloads;
import io.github.alkarim786.shop.events.Topics;
import io.github.alkarim786.shop.messaging.OutboxWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.sql.Timestamp;
import java.time.Clock;
import java.util.UUID;

/**
 * Charges the customer once stock is reserved. Consumes only inventory.events: StockReserved
 * carries amount, customer and token, so this service never depends on cross-topic ordering.
 */
@Component
public class PaymentHandler {

    public static final String CONSUMER_GROUP = "payment-service";
    private static final Logger log = LoggerFactory.getLogger(PaymentHandler.class);

    private final JdbcTemplate jdbc;
    private final PaymentGateway gateway;
    private final OutboxWriter outbox;
    private final EventCodec codec;
    private final Clock clock;

    public PaymentHandler(JdbcTemplate jdbc, PaymentGateway gateway, OutboxWriter outbox, EventCodec codec, Clock clock) {
        this.jdbc = jdbc;
        this.gateway = gateway;
        this.outbox = outbox;
        this.codec = codec;
        this.clock = clock;
    }

    public void handle(EventEnvelope event) {
        if (!EventTypes.STOCK_RESERVED.equals(event.type())) {
            log.debug("payment-service ignores {}", event.type());
            return;
        }
        var reserved = codec.payload(event, Payloads.StockReserved.class);
        Integer existing = jdbc.queryForObject("SELECT COUNT(*) FROM payments WHERE order_id = ?",
                Integer.class, reserved.orderId());
        if (existing != null && existing > 0) {
            return; // already charged (or declined) for this order
        }

        var result = gateway.charge(reserved.orderId(), reserved.customerId(), reserved.totalCents(),
                reserved.paymentToken());
        UUID paymentId = UUID.randomUUID();
        jdbc.update("""
                INSERT INTO payments (id, order_id, customer_id, amount_cents, status, failure_reason, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)""",
                paymentId, reserved.orderId(), reserved.customerId(), reserved.totalCents(),
                result.approved() ? "COMPLETED" : "FAILED", result.declineReason(), Timestamp.from(clock.instant()));

        if (result.approved()) {
            outbox.append(Topics.PAYMENTS, EventTypes.PAYMENT_COMPLETED, reserved.orderId(),
                    new Payloads.PaymentCompleted(reserved.orderId(), paymentId.toString(), reserved.totalCents()));
        } else {
            outbox.append(Topics.PAYMENTS, EventTypes.PAYMENT_FAILED, reserved.orderId(),
                    new Payloads.PaymentFailed(reserved.orderId(), paymentId.toString(), result.declineReason()));
        }
    }
}
