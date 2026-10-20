package io.github.alkarim786.shop.system;

import io.github.alkarim786.shop.events.EventEnvelope;
import io.github.alkarim786.shop.events.EventTypes;
import io.github.alkarim786.shop.events.Topics;
import io.github.alkarim786.shop.messaging.testing.RecordingPublisher;
import io.github.alkarim786.shop.order.OrderDtos.ItemRequest;
import io.github.alkarim786.shop.order.OrderDtos.OrderView;
import io.github.alkarim786.shop.order.OrderDtos.PlaceOrderRequest;
import io.github.alkarim786.shop.order.OrderDtos.TimelineEntry;
import io.github.alkarim786.shop.order.OrderStatus;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;

/** Full choreography saga across all four services, in-process (no Docker needed). */
class InMemorySagaTest {

    private static PlaceOrderRequest order(String token, ItemRequest... items) {
        return new PlaceOrderRequest("cust-1", List.of(items), token, null);
    }

    private static OrderView view(InMemoryShop shop, UUID id) {
        return shop.orders.find(id).orElseThrow();
    }

    private static Map<String, Object> product(InMemoryShop shop, String sku) {
        return shop.inventoryDb.jdbc.queryForMap("SELECT available, reserved, sold FROM products WHERE sku = ?", sku);
    }

    @Test
    void happyPathConfirmsOrderChargesOnceAndNotifies() {
        try (var shop = new InMemoryShop(1, 0)) {
            UUID id = shop.orders.placeOrder(order(null, new ItemRequest("MOUSE-02", 2)), null).orderId();
            shop.runUntilQuiet();

            OrderView v = view(shop, id);
            assertThat(v.status()).isEqualTo(OrderStatus.CONFIRMED);
            assertThat(v.timeline()).extracting(TimelineEntry::event).containsExactly(
                    EventTypes.ORDER_CREATED, EventTypes.STOCK_RESERVED, EventTypes.PAYMENT_COMPLETED,
                    EventTypes.ORDER_CONFIRMED, EventTypes.STOCK_COMMITTED);
            assertThat(product(shop, "MOUSE-02")).containsEntry("available", 99_998)
                    .containsEntry("reserved", 0).containsEntry("sold", 2);
            assertThat(shop.paymentsDb.jdbc.queryForObject("SELECT status FROM payments WHERE order_id = ?",
                    String.class, id.toString())).isEqualTo("COMPLETED");
            assertThat(shop.notificationsDb.count("SELECT COUNT(*) FROM notifications WHERE event_type = 'OrderConfirmed'"))
                    .isEqualTo(1);
        }
    }

    @Test
    void paymentFailureCompensatesInventoryAndCancelsOrder() {
        try (var shop = new InMemoryShop(1, 0)) {
            UUID id = shop.orders.placeOrder(order("tok_decline", new ItemRequest("HOT-DROP", 5)), null).orderId();
            shop.runUntilQuiet();

            OrderView v = view(shop, id);
            assertThat(v.status()).isEqualTo(OrderStatus.CANCELLED);
            assertThat(v.cancelReason()).isEqualTo("payment failed: card_declined");
            assertThat(v.timeline()).extracting(TimelineEntry::event).contains(
                    EventTypes.STOCK_RESERVED, EventTypes.PAYMENT_FAILED, EventTypes.ORDER_CANCELLED,
                    EventTypes.STOCK_RELEASED);
            // Compensation put the 5 units back.
            assertThat(product(shop, "HOT-DROP")).containsEntry("available", 50)
                    .containsEntry("reserved", 0).containsEntry("sold", 0);
            assertThat(shop.notificationsDb.jdbc.queryForObject("SELECT message FROM notifications", String.class))
                    .contains("cancelled");
        }
    }

    @Test
    void outOfStockCancelsWithoutEverCharging() {
        try (var shop = new InMemoryShop(1, 0)) {
            UUID id = shop.orders.placeOrder(order(null, new ItemRequest("KB-MECH-01", 1),
                    new ItemRequest("HOT-DROP", 51)), null).orderId();
            shop.runUntilQuiet();

            assertThat(view(shop, id).status()).isEqualTo(OrderStatus.CANCELLED);
            assertThat(shop.paymentsDb.count("SELECT COUNT(*) FROM payments")).isZero();
            assertThat(product(shop, "KB-MECH-01")).containsEntry("available", 100_000).containsEntry("reserved", 0);
        }
    }

    @Test
    void redeliveryStormHasExactlyOnceEffects() {
        // Every message is published twice by the relay (chaos) AND delivered 3x by the bus.
        try (var shop = new InMemoryShop(3, 1.0)) {
            List<UUID> ids = new ArrayList<>();
            for (int i = 0; i < 30; i++) {
                String token = i % 5 == 0 ? "tok_decline" : "tok_ok";
                ids.add(shop.orders.placeOrder(order(token, new ItemRequest("HOT-DROP", 1),
                        new ItemRequest("USB-C-HUB", 2)), null).orderId());
            }
            shop.runUntilQuiet();

            long confirmed = ids.stream().filter(id -> view(shop, id).status() == OrderStatus.CONFIRMED).count();
            assertThat(confirmed).isEqualTo(24);
            assertThat(ids).allMatch(id -> view(shop, id).status().isTerminal());
            assertThat(shop.chaosDuplicates()).isPositive();

            // Exactly one payment and one notification per order, despite ~6x delivery.
            assertThat(shop.paymentsDb.count("SELECT COUNT(*) FROM payments")).isEqualTo(30);
            assertThat(shop.notificationsDb.count("SELECT COUNT(*) FROM notifications")).isEqualTo(30);
            assertThat(product(shop, "HOT-DROP")).containsEntry("sold", 24).containsEntry("reserved", 0)
                    .containsEntry("available", 26);
            assertThat(product(shop, "USB-C-HUB")).containsEntry("sold", 48).containsEntry("available", 100_000 - 48);
        }
    }

    @Test
    void flashSaleRaceNeverOversells() throws Exception {
        try (var shop = new InMemoryShop(1, 0)) {
            int buyers = 40; // 40 buyers x 2 units each, only 50 units in stock
            List<UUID> ids = new ArrayList<>();
            for (int i = 0; i < buyers; i++) {
                ids.add(shop.orders.placeOrder(order(null, new ItemRequest("HOT-DROP", 2)), null).orderId());
            }
            // Deliver all OrderCreated events to inventory concurrently, as 3 partitions' consumers would.
            List<RecordingPublisher.Message> created = shop.relayAll();
            var inventory = shop.subscribers(Topics.ORDERS).getFirst();
            ExecutorService pool = Executors.newFixedThreadPool(8);
            CountDownLatch start = new CountDownLatch(1);
            try {
                List<Future<?>> fs = new ArrayList<>();
                for (RecordingPublisher.Message m : created) {
                    EventEnvelope env = shop.codec.parse(m.value());
                    fs.add(pool.submit(() -> {
                        start.await();
                        inventory.deliver(env);
                        return null;
                    }));
                }
                start.countDown();
                for (Future<?> f : fs) {
                    f.get();
                }
            } finally {
                pool.shutdownNow();
            }
            shop.runUntilQuiet();

            long confirmed = ids.stream().filter(id -> view(shop, id).status() == OrderStatus.CONFIRMED).count();
            assertThat(confirmed).isEqualTo(25);
            assertThat(product(shop, "HOT-DROP")).containsEntry("available", 0)
                    .containsEntry("reserved", 0).containsEntry("sold", 50);
        }
    }
}
