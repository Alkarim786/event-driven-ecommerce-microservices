package io.github.alkarim786.shop.inventory;

import io.github.alkarim786.shop.events.EventCodec;
import io.github.alkarim786.shop.events.EventEnvelope;
import io.github.alkarim786.shop.events.EventTypes;
import io.github.alkarim786.shop.events.Payloads;
import io.github.alkarim786.shop.events.Payloads.LineItem;
import io.github.alkarim786.shop.messaging.IdempotentConsumer;
import io.github.alkarim786.shop.messaging.OutboxWriter;
import io.github.alkarim786.shop.messaging.testing.TestDatabase;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;

class InventoryHandlerTest {

    private final TestDatabase db = TestDatabase.h2("classpath:db/inventory");
    private final EventCodec codec = new EventCodec();
    private final InventoryHandler handler = new InventoryHandler(db.jdbc, new OutboxWriter(db.jdbc, codec), codec,
            Clock.systemUTC());
    private final IdempotentConsumer consumer = new IdempotentConsumer(db.jdbc, db.tx, Clock.systemUTC(), 50);

    @AfterEach
    void close() {
        db.close();
    }

    private EventEnvelope orderCreated(String orderId, LineItem... items) {
        return codec.envelope(EventTypes.ORDER_CREATED, orderId,
                new Payloads.OrderCreated(orderId, "cust", List.of(items), 1000, "tok_ok"));
    }

    private void deliver(EventEnvelope env) {
        consumer.process(InventoryHandler.CONSUMER_GROUP, env, handler::handle);
    }

    private Map<String, Object> product(String sku) {
        return db.jdbc.queryForMap("SELECT available, reserved, sold FROM products WHERE sku = ?", sku);
    }

    private List<String> outboxTypes() {
        return db.jdbc.queryForList("SELECT event_type FROM outbox ORDER BY seq", String.class);
    }

    @Test
    void reservesAllItemsAndEmitsStockReservedWithPaymentContext() {
        deliver(orderCreated("o-1", new LineItem("MOUSE-02", 2), new LineItem("KB-MECH-01", 1)));

        assertThat(product("MOUSE-02")).containsEntry("available", 99_998).containsEntry("reserved", 2);
        assertThat(product("KB-MECH-01")).containsEntry("reserved", 1);
        assertThat(outboxTypes()).containsExactly(EventTypes.STOCK_RESERVED);
        var reserved = codec.payload(codec.parse(db.jdbc.queryForObject("SELECT payload FROM outbox", String.class)),
                Payloads.StockReserved.class);
        assertThat(reserved.totalCents()).isEqualTo(1000);
        assertThat(reserved.paymentToken()).isEqualTo("tok_ok");
    }

    @Test
    void shortageOnAnyItemRejectsWholeOrderWithoutPartialReservation() {
        deliver(orderCreated("o-1", new LineItem("MOUSE-02", 2), new LineItem("HOT-DROP", 51)));

        assertThat(product("MOUSE-02")).containsEntry("available", 100_000).containsEntry("reserved", 0);
        assertThat(product("HOT-DROP")).containsEntry("available", 50);
        assertThat(outboxTypes()).containsExactly(EventTypes.STOCK_REJECTED);
        assertThat(db.count("SELECT COUNT(*) FROM reservations")).isZero();
    }

    @Test
    void paymentFailedReleasesReservationExactlyOnce() {
        deliver(orderCreated("o-1", new LineItem("HOT-DROP", 5)));
        var failed = codec.envelope(EventTypes.PAYMENT_FAILED, "o-1", new Payloads.PaymentFailed("o-1", "p", "card_declined"));
        deliver(failed);
        deliver(failed); // duplicate delivery
        // a distinct second PaymentFailed: guarded by reservation status, still no double release
        deliver(codec.envelope(EventTypes.PAYMENT_FAILED, "o-1", new Payloads.PaymentFailed("o-1", "p", "x")));

        assertThat(product("HOT-DROP")).containsEntry("available", 50).containsEntry("reserved", 0);
        assertThat(outboxTypes()).containsExactly(EventTypes.STOCK_RESERVED, EventTypes.STOCK_RELEASED);
    }

    @Test
    void paymentCompletedMovesReservedToSold() {
        deliver(orderCreated("o-1", new LineItem("HOT-DROP", 3)));
        deliver(codec.envelope(EventTypes.PAYMENT_COMPLETED, "o-1", new Payloads.PaymentCompleted("o-1", "p", 1000)));

        assertThat(product("HOT-DROP")).containsEntry("available", 47).containsEntry("reserved", 0).containsEntry("sold", 3);
        assertThat(outboxTypes()).containsExactly(EventTypes.STOCK_RESERVED, EventTypes.STOCK_COMMITTED);
    }

    @Test
    void duplicateOrderCreatedReservesOnce() {
        EventEnvelope env = orderCreated("o-1", new LineItem("HOT-DROP", 1));
        deliver(env);
        deliver(env);
        assertThat(product("HOT-DROP")).containsEntry("available", 49).containsEntry("reserved", 1);
    }

    @Test
    void concurrentOrdersForTheLastUnitsNeverOversell() throws Exception {
        db.jdbc.update("INSERT INTO products (sku, name, available) VALUES ('LAST-ONE', 'last unit', 3)");
        int buyers = 24;
        ExecutorService pool = Executors.newFixedThreadPool(8);
        CountDownLatch start = new CountDownLatch(1);
        try {
            List<Future<?>> futures = new ArrayList<>();
            for (int i = 0; i < buyers; i++) {
                EventEnvelope env = orderCreated("o-" + i, new LineItem("LAST-ONE", 1));
                futures.add(pool.submit(() -> {
                    start.await();
                    deliver(env);
                    return null;
                }));
            }
            start.countDown();
            for (Future<?> f : futures) {
                f.get();
            }
        } finally {
            pool.shutdownNow();
        }

        assertThat(product("LAST-ONE")).containsEntry("available", 0).containsEntry("reserved", 3);
        assertThat(outboxTypes()).filteredOn(EventTypes.STOCK_RESERVED::equals).hasSize(3);
        assertThat(outboxTypes()).filteredOn(EventTypes.STOCK_REJECTED::equals).hasSize(buyers - 3);
    }
}
