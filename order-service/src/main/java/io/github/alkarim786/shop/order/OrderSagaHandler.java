package io.github.alkarim786.shop.order;

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
import java.util.List;
import java.util.UUID;

/**
 * order-service's side of the choreography saga. Runs inside the idempotent-consumer transaction,
 * so the status change, the timeline entry and the follow-up event commit atomically.
 */
@Component
public class OrderSagaHandler {

    public static final String CONSUMER_GROUP = "order-service";
    private static final Logger log = LoggerFactory.getLogger(OrderSagaHandler.class);

    private final JdbcTemplate jdbc;
    private final OrderService orders;
    private final OutboxWriter outbox;
    private final EventCodec codec;
    private final Clock clock;

    public OrderSagaHandler(JdbcTemplate jdbc, OrderService orders, OutboxWriter outbox, EventCodec codec, Clock clock) {
        this.jdbc = jdbc;
        this.orders = orders;
        this.outbox = outbox;
        this.codec = codec;
        this.clock = clock;
    }

    public void handle(EventEnvelope event) {
        UUID orderId = UUID.fromString(event.aggregateId());
        switch (event.type()) {
            case EventTypes.STOCK_RESERVED -> orders.addTimeline(orderId, event.type(), null, event.occurredAt());
            case EventTypes.STOCK_RELEASED -> orders.addTimeline(orderId, event.type(), "compensated", event.occurredAt());
            case EventTypes.STOCK_COMMITTED -> orders.addTimeline(orderId, event.type(), null, event.occurredAt());
            case EventTypes.STOCK_REJECTED -> {
                var p = codec.payload(event, Payloads.StockRejected.class);
                orders.addTimeline(orderId, event.type(), p.reason(), event.occurredAt());
                complete(orderId, OrderStatus.CANCELLED, p.reason());
            }
            case EventTypes.PAYMENT_COMPLETED -> {
                var p = codec.payload(event, Payloads.PaymentCompleted.class);
                orders.addTimeline(orderId, event.type(), "paymentId=" + p.paymentId(), event.occurredAt());
                complete(orderId, OrderStatus.CONFIRMED, null);
            }
            case EventTypes.PAYMENT_FAILED -> {
                var p = codec.payload(event, Payloads.PaymentFailed.class);
                orders.addTimeline(orderId, event.type(), p.reason(), event.occurredAt());
                complete(orderId, OrderStatus.CANCELLED, "payment failed: " + p.reason());
            }
            default -> log.debug("order-service ignores {}", event.type());
        }
    }

    /**
     * Compare-and-set on status: only a PENDING order can complete. A late or replayed event for a
     * terminal order updates 0 rows and emits nothing, so the outcome can never flip.
     */
    private void complete(UUID orderId, OrderStatus target, String reason) {
        int updated = jdbc.update("""
                UPDATE orders SET status = ?, cancel_reason = ?, completed_at = ?, version = version + 1
                WHERE id = ? AND status = ?""",
                target.name(), reason, Timestamp.from(clock.instant()), orderId, OrderStatus.PENDING.name());
        if (updated == 0) {
            log.info("order {} already terminal; ignoring transition to {}", orderId, target);
            return;
        }
        List<Object[]> row = jdbc.query("SELECT customer_id, total_cents FROM orders WHERE id = ?",
                (rs, i) -> new Object[]{rs.getString(1), rs.getLong(2)}, orderId);
        String customerId = (String) row.getFirst()[0];
        long total = (Long) row.getFirst()[1];
        if (target == OrderStatus.CONFIRMED) {
            outbox.append(Topics.ORDERS, EventTypes.ORDER_CONFIRMED, orderId.toString(),
                    new Payloads.OrderConfirmed(orderId.toString(), customerId, total));
        } else {
            outbox.append(Topics.ORDERS, EventTypes.ORDER_CANCELLED, orderId.toString(),
                    new Payloads.OrderCancelled(orderId.toString(), customerId, reason));
        }
        orders.addTimeline(orderId, target == OrderStatus.CONFIRMED ? EventTypes.ORDER_CONFIRMED
                : EventTypes.ORDER_CANCELLED, reason, clock.instant());
    }
}
