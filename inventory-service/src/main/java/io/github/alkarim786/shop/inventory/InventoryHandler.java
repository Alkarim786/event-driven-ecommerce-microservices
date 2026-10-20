package io.github.alkarim786.shop.inventory;

import io.github.alkarim786.shop.events.EventCodec;
import io.github.alkarim786.shop.events.EventEnvelope;
import io.github.alkarim786.shop.events.EventTypes;
import io.github.alkarim786.shop.events.Payloads;
import io.github.alkarim786.shop.events.Payloads.LineItem;
import io.github.alkarim786.shop.events.Topics;
import io.github.alkarim786.shop.messaging.OutboxWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.sql.Timestamp;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * inventory-service's side of the saga.
 *
 * <p>Reservation is all-or-nothing and two-phase inside one transaction:
 * <ol>
 *   <li><b>check</b>: read every SKU (with its version) and decide. If anything is short, emit
 *       StockRejected having written nothing, so there is never a partial reservation to undo.</li>
 *   <li><b>apply</b>: {@code UPDATE ... WHERE sku = ? AND version = ?}. If another order changed
 *       the row since we read it, 0 rows update and we throw
 *       {@link OptimisticLockingFailureException}; the idempotent consumer rolls back and retries
 *       the whole decision against fresh data.</li>
 * </ol>
 * SKUs are processed in sorted order so concurrent multi-item orders touch rows in the same order.
 */
@Component
public class InventoryHandler {

    public static final String CONSUMER_GROUP = "inventory-service";
    private static final Logger log = LoggerFactory.getLogger(InventoryHandler.class);

    private final JdbcTemplate jdbc;
    private final OutboxWriter outbox;
    private final EventCodec codec;
    private final Clock clock;

    public InventoryHandler(JdbcTemplate jdbc, OutboxWriter outbox, EventCodec codec, Clock clock) {
        this.jdbc = jdbc;
        this.outbox = outbox;
        this.codec = codec;
        this.clock = clock;
    }

    public void handle(EventEnvelope event) {
        switch (event.type()) {
            case EventTypes.ORDER_CREATED -> reserve(codec.payload(event, Payloads.OrderCreated.class));
            case EventTypes.PAYMENT_FAILED -> release(codec.payload(event, Payloads.PaymentFailed.class).orderId());
            case EventTypes.PAYMENT_COMPLETED -> commit(codec.payload(event, Payloads.PaymentCompleted.class).orderId());
            default -> log.debug("inventory-service ignores {}", event.type());
        }
    }

    private record Stock(int available, long version) {}

    void reserve(Payloads.OrderCreated order) {
        if (hasReservations(order.orderId())) {
            return; // business-level idempotency, on top of the processed_messages guard
        }
        Map<String, Integer> wanted = new TreeMap<>();
        for (LineItem item : order.items()) {
            wanted.merge(item.sku(), item.quantity(), Integer::sum);
        }

        // Phase 1: check everything before writing anything.
        Map<String, Stock> stock = new TreeMap<>();
        for (var e : wanted.entrySet()) {
            List<Stock> rows = jdbc.query("SELECT available, version FROM products WHERE sku = ?",
                    (rs, i) -> new Stock(rs.getInt(1), rs.getLong(2)), e.getKey());
            if (rows.isEmpty()) {
                reject(order.orderId(), "unknown sku " + e.getKey());
                return;
            }
            if (rows.getFirst().available() < e.getValue()) {
                reject(order.orderId(), "out of stock: " + e.getKey());
                return;
            }
            stock.put(e.getKey(), rows.getFirst());
        }

        // Phase 2: apply with optimistic locking.
        Timestamp now = Timestamp.from(clock.instant());
        List<LineItem> reserved = new ArrayList<>();
        for (var e : wanted.entrySet()) {
            Stock s = stock.get(e.getKey());
            int updated = jdbc.update("""
                    UPDATE products SET available = available - ?, reserved = reserved + ?, version = version + 1
                    WHERE sku = ? AND version = ?""", e.getValue(), e.getValue(), e.getKey(), s.version());
            if (updated == 0) {
                throw new OptimisticLockingFailureException("product " + e.getKey() + " changed concurrently");
            }
            jdbc.update("INSERT INTO reservations (order_id, sku, quantity, status, updated_at) VALUES (?, ?, ?, 'RESERVED', ?)",
                    order.orderId(), e.getKey(), e.getValue(), now);
            reserved.add(new LineItem(e.getKey(), e.getValue()));
        }
        outbox.append(Topics.INVENTORY, EventTypes.STOCK_RESERVED, order.orderId(),
                new Payloads.StockReserved(order.orderId(), order.customerId(), reserved,
                        order.totalCents(), order.paymentToken()));
    }

    private void reject(String orderId, String reason) {
        outbox.append(Topics.INVENTORY, EventTypes.STOCK_REJECTED, orderId,
                new Payloads.StockRejected(orderId, reason));
    }

    /** Compensation for a failed payment: put reserved units back on the shelf. */
    void release(String orderId) {
        List<LineItem> items = moveReservations(orderId, "RELEASED",
                "UPDATE products SET available = available + ?, reserved = reserved - ?, version = version + 1 WHERE sku = ?");
        if (!items.isEmpty()) {
            outbox.append(Topics.INVENTORY, EventTypes.STOCK_RELEASED, orderId,
                    new Payloads.StockReleased(orderId, items));
        }
    }

    /** Payment succeeded: reserved units become sold. */
    void commit(String orderId) {
        List<LineItem> items = moveReservations(orderId, "COMMITTED",
                "UPDATE products SET sold = sold + ?, reserved = reserved - ?, version = version + 1 WHERE sku = ?");
        if (!items.isEmpty()) {
            outbox.append(Topics.INVENTORY, EventTypes.STOCK_COMMITTED, orderId,
                    new Payloads.StockCommitted(orderId, items));
        }
    }

    /**
     * Moves RESERVED rows of an order to {@code target}. The guarded UPDATE on reservations is
     * the state-machine check: a second release/commit finds nothing in RESERVED and does nothing.
     * Product updates are relative ({@code x = x + ?}) and take a row lock, so they cannot lose
     * updates; bumping version makes concurrent optimistic reservers re-read.
     */
    private List<LineItem> moveReservations(String orderId, String target, String productUpdate) {
        List<LineItem> items = jdbc.query(
                "SELECT sku, quantity FROM reservations WHERE order_id = ? AND status = 'RESERVED' ORDER BY sku",
                (rs, i) -> new LineItem(rs.getString(1), rs.getInt(2)), orderId);
        Timestamp now = Timestamp.from(clock.instant());
        for (LineItem item : items) {
            int moved = jdbc.update("""
                    UPDATE reservations SET status = ?, updated_at = ?
                    WHERE order_id = ? AND sku = ? AND status = 'RESERVED'""", target, now, orderId, item.sku());
            if (moved == 1) {
                jdbc.update(productUpdate, item.quantity(), item.quantity(), item.sku());
            }
        }
        return items;
    }

    private boolean hasReservations(String orderId) {
        Integer n = jdbc.queryForObject("SELECT COUNT(*) FROM reservations WHERE order_id = ?", Integer.class, orderId);
        return n != null && n > 0;
    }
}
