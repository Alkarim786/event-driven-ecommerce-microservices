package io.github.alkarim786.shop.order;

import io.github.alkarim786.shop.events.EventTypes;
import io.github.alkarim786.shop.events.Payloads;
import io.github.alkarim786.shop.events.Topics;
import io.github.alkarim786.shop.messaging.OutboxWriter;
import io.github.alkarim786.shop.order.OrderDtos.ItemRequest;
import io.github.alkarim786.shop.order.OrderDtos.ItemView;
import io.github.alkarim786.shop.order.OrderDtos.OrderView;
import io.github.alkarim786.shop.order.OrderDtos.PlaceOrderRequest;
import io.github.alkarim786.shop.order.OrderDtos.PlaceOrderResponse;
import io.github.alkarim786.shop.order.OrderDtos.TimelineEntry;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

import java.sql.Timestamp;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/** Places orders (start of the saga) and serves the order read model. */
@Service
public class OrderService {

    private final JdbcTemplate jdbc;
    private final TransactionTemplate tx;
    private final OutboxWriter outbox;
    private final Clock clock;

    public OrderService(JdbcTemplate jdbc, TransactionTemplate tx, OutboxWriter outbox, Clock clock) {
        this.jdbc = jdbc;
        this.tx = tx;
        this.outbox = outbox;
        this.clock = clock;
    }

    /**
     * Inserts the order and its OrderCreated event in ONE local transaction (transactional outbox).
     * With an idempotency key, a retried request returns the original order instead of a second one.
     */
    public PlaceOrderResponse placeOrder(PlaceOrderRequest req, String idempotencyKey) {
        if (idempotencyKey != null) {
            Optional<PlaceOrderResponse> existing = findByIdempotencyKey(idempotencyKey);
            if (existing.isPresent()) {
                return existing.get();
            }
        }
        try {
            return tx.execute(status -> insertOrder(req, idempotencyKey));
        } catch (DuplicateKeyException e) {
            // Lost a race with a concurrent retry carrying the same key: return the winner.
            return findByIdempotencyKey(idempotencyKey).orElseThrow(() -> e);
        }
    }

    private PlaceOrderResponse insertOrder(PlaceOrderRequest req, String idempotencyKey) {
        Map<String, Integer> quantities = new LinkedHashMap<>();
        for (ItemRequest item : req.items()) {
            quantities.merge(item.sku(), item.quantity(), Integer::sum);
        }
        Map<String, Long> prices = loadPrices(quantities.keySet());

        UUID orderId = UUID.randomUUID();
        Instant now = clock.instant();
        long total = 0;
        List<Payloads.LineItem> lines = new ArrayList<>();
        for (var e : quantities.entrySet()) {
            total += prices.get(e.getKey()) * e.getValue();
            lines.add(new Payloads.LineItem(e.getKey(), e.getValue()));
        }
        String token = req.effectiveToken();

        jdbc.update("""
                INSERT INTO orders (id, customer_id, status, total_cents, payment_token, idempotency_key, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)""",
                orderId, req.customerId(), OrderStatus.PENDING.name(), total, token, idempotencyKey,
                Timestamp.from(now));
        for (Payloads.LineItem line : lines) {
            jdbc.update("INSERT INTO order_items (order_id, sku, quantity, unit_price_cents) VALUES (?, ?, ?, ?)",
                    orderId, line.sku(), line.quantity(), prices.get(line.sku()));
        }
        addTimeline(orderId, EventTypes.ORDER_CREATED, "total=" + total, now);
        outbox.append(Topics.ORDERS, EventTypes.ORDER_CREATED, orderId.toString(),
                new Payloads.OrderCreated(orderId.toString(), req.customerId(), lines, total, token));
        return new PlaceOrderResponse(orderId, OrderStatus.PENDING);
    }

    private Map<String, Long> loadPrices(Iterable<String> skus) {
        Map<String, Long> prices = new LinkedHashMap<>();
        for (String sku : skus) {
            List<Long> found = jdbc.queryForList("SELECT price_cents FROM catalog WHERE sku = ?", Long.class, sku);
            if (found.isEmpty()) {
                throw new UnknownSkuException(sku);
            }
            prices.put(sku, found.getFirst());
        }
        return prices;
    }

    void addTimeline(UUID orderId, String eventType, String detail, Instant at) {
        jdbc.update("INSERT INTO order_timeline (order_id, event_type, detail, occurred_at) VALUES (?, ?, ?, ?)",
                orderId, eventType, detail, Timestamp.from(at));
    }

    private Optional<PlaceOrderResponse> findByIdempotencyKey(String key) {
        return jdbc.query("SELECT id, status FROM orders WHERE idempotency_key = ?",
                        (rs, i) -> new PlaceOrderResponse(rs.getObject(1, UUID.class),
                                OrderStatus.valueOf(rs.getString(2))), key)
                .stream().findFirst();
    }

    /**
     * Reads the order, then its items and timeline as separate queries AFTER the first result set
     * is closed. Running them inside the row mapper would make each request hold one pooled
     * connection while borrowing a second; under load every connection ends up held by a request
     * waiting for another, and the pool deadlocks (the load test hit exactly this).
     */
    public Optional<OrderView> find(UUID orderId) {
        List<OrderView> found = jdbc.query("""
                SELECT customer_id, status, total_cents, cancel_reason, created_at, completed_at
                FROM orders WHERE id = ?""", (rs, i) -> {
            Instant created = rs.getTimestamp(5).toInstant();
            Timestamp completedTs = rs.getTimestamp(6);
            Instant completed = completedTs == null ? null : completedTs.toInstant();
            Long latency = completed == null ? null : Duration.between(created, completed).toMillis();
            return new OrderView(orderId, rs.getString(1), OrderStatus.valueOf(rs.getString(2)),
                    rs.getLong(3), rs.getString(4), created, completed, latency, List.of(), List.of());
        }, orderId);
        return found.stream().findFirst().map(o -> new OrderView(o.orderId(), o.customerId(), o.status(),
                o.totalCents(), o.cancelReason(), o.createdAt(), o.completedAt(), o.sagaLatencyMs(),
                items(orderId), timeline(orderId)));
    }

    private List<ItemView> items(UUID orderId) {
        return jdbc.query("SELECT sku, quantity, unit_price_cents FROM order_items WHERE order_id = ? ORDER BY sku",
                (rs, i) -> new ItemView(rs.getString(1), rs.getInt(2), rs.getLong(3)), orderId);
    }

    private List<TimelineEntry> timeline(UUID orderId) {
        return jdbc.query("SELECT event_type, detail, occurred_at FROM order_timeline WHERE order_id = ? ORDER BY id",
                (rs, i) -> new TimelineEntry(rs.getString(1), rs.getString(2), rs.getTimestamp(3).toInstant()),
                orderId);
    }

    public static class UnknownSkuException extends RuntimeException {
        public UnknownSkuException(String sku) {
            super("unknown sku: " + sku);
        }
    }
}
