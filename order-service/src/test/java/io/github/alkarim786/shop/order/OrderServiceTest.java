package io.github.alkarim786.shop.order;

import io.github.alkarim786.shop.events.EventCodec;
import io.github.alkarim786.shop.events.EventEnvelope;
import io.github.alkarim786.shop.events.EventTypes;
import io.github.alkarim786.shop.events.Payloads;
import io.github.alkarim786.shop.messaging.IdempotentConsumer;
import io.github.alkarim786.shop.messaging.OutboxWriter;
import io.github.alkarim786.shop.messaging.testing.TestDatabase;
import io.github.alkarim786.shop.order.OrderDtos.ItemRequest;
import io.github.alkarim786.shop.order.OrderDtos.PlaceOrderRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class OrderServiceTest {

    private final TestDatabase db = TestDatabase.h2("classpath:db/orders");
    private final EventCodec codec = new EventCodec();
    private final OutboxWriter outbox = new OutboxWriter(db.jdbc, codec);
    private final OrderService orders = new OrderService(db.jdbc, db.tx, outbox, Clock.systemUTC());
    private final OrderSagaHandler saga = new OrderSagaHandler(db.jdbc, orders, outbox, codec, Clock.systemUTC());
    private final IdempotentConsumer consumer = new IdempotentConsumer(db.jdbc, db.tx, Clock.systemUTC(), 5);

    @AfterEach
    void close() {
        db.close();
    }

    private PlaceOrderRequest request(String token) {
        return new PlaceOrderRequest("cust-1",
                List.of(new ItemRequest("MOUSE-02", 2), new ItemRequest("USB-C-HUB", 1), new ItemRequest("MOUSE-02", 1)),
                token, null);
    }

    private List<String> outboxTypes() {
        return db.jdbc.queryForList("SELECT event_type FROM outbox ORDER BY seq", String.class);
    }

    private void deliver(String type, UUID orderId, Object payload) {
        EventEnvelope env = codec.envelope(type, orderId.toString(), payload);
        consumer.process(OrderSagaHandler.CONSUMER_GROUP, env, saga::handle);
    }

    @Test
    void placeOrderPricesFromCatalogMergesLinesAndWritesOrderCreatedAtomically() {
        var resp = orders.placeOrder(request(null), null);

        var view = orders.find(resp.orderId()).orElseThrow();
        assertThat(view.status()).isEqualTo(OrderStatus.PENDING);
        assertThat(view.totalCents()).isEqualTo(3 * 129900 + 249900);
        assertThat(view.items()).extracting(OrderDtos.ItemView::quantity).containsExactly(3, 1);
        assertThat(outboxTypes()).containsExactly(EventTypes.ORDER_CREATED);

        String json = db.jdbc.queryForObject("SELECT payload FROM outbox", String.class);
        var created = codec.payload(codec.parse(json), Payloads.OrderCreated.class);
        assertThat(created.paymentToken()).isEqualTo("tok_ok");
        assertThat(created.totalCents()).isEqualTo(view.totalCents());
    }

    @Test
    void unknownSkuRejectsTheWholeOrderAndWritesNothing() {
        var req = new PlaceOrderRequest("cust-1", List.of(new ItemRequest("MOUSE-02", 1), new ItemRequest("NOPE", 1)),
                null, null);
        assertThatThrownBy(() -> orders.placeOrder(req, null)).isInstanceOf(OrderService.UnknownSkuException.class);
        assertThat(db.count("SELECT COUNT(*) FROM orders")).isZero();
        assertThat(db.count("SELECT COUNT(*) FROM outbox")).isZero();
    }

    @Test
    void sameIdempotencyKeyReturnsTheOriginalOrder() {
        var first = orders.placeOrder(request(null), "key-123");
        var retry = orders.placeOrder(request(null), "key-123");
        assertThat(retry.orderId()).isEqualTo(first.orderId());
        assertThat(db.count("SELECT COUNT(*) FROM orders")).isEqualTo(1);
        assertThat(db.count("SELECT COUNT(*) FROM outbox")).isEqualTo(1);
    }

    @Test
    void simulatePaymentFailureFlagMapsToDeclineToken() {
        var req = new PlaceOrderRequest("c", List.of(new ItemRequest("MOUSE-02", 1)), null, true);
        assertThat(req.effectiveToken()).isEqualTo("tok_decline");
    }

    @Test
    void paymentCompletedConfirmsOrderAndEmitsOrderConfirmed() {
        UUID id = orders.placeOrder(request(null), null).orderId();
        deliver(EventTypes.STOCK_RESERVED, id, new Payloads.StockReserved(id.toString(), "cust-1", List.of(), 1, "tok_ok"));
        deliver(EventTypes.PAYMENT_COMPLETED, id, new Payloads.PaymentCompleted(id.toString(), "p-1", 1));

        var view = orders.find(id).orElseThrow();
        assertThat(view.status()).isEqualTo(OrderStatus.CONFIRMED);
        assertThat(view.completedAt()).isNotNull();
        assertThat(view.sagaLatencyMs()).isNotNull();
        assertThat(view.timeline()).extracting(OrderDtos.TimelineEntry::event).containsExactly(
                EventTypes.ORDER_CREATED, EventTypes.STOCK_RESERVED, EventTypes.PAYMENT_COMPLETED, EventTypes.ORDER_CONFIRMED);
        assertThat(outboxTypes()).containsExactly(EventTypes.ORDER_CREATED, EventTypes.ORDER_CONFIRMED);
    }

    @Test
    void stockRejectedCancelsOrderWithReason() {
        UUID id = orders.placeOrder(request(null), null).orderId();
        deliver(EventTypes.STOCK_REJECTED, id, new Payloads.StockRejected(id.toString(), "out of stock: MOUSE-02"));

        var view = orders.find(id).orElseThrow();
        assertThat(view.status()).isEqualTo(OrderStatus.CANCELLED);
        assertThat(view.cancelReason()).isEqualTo("out of stock: MOUSE-02");
        assertThat(outboxTypes()).containsExactly(EventTypes.ORDER_CREATED, EventTypes.ORDER_CANCELLED);
    }

    @Test
    void terminalOrderNeverFlipsAndEmitsNothingMore() {
        UUID id = orders.placeOrder(request(null), null).orderId();
        deliver(EventTypes.PAYMENT_COMPLETED, id, new Payloads.PaymentCompleted(id.toString(), "p-1", 1));
        // A distinct (not duplicate) conflicting event arrives late: it must not cancel a confirmed order.
        deliver(EventTypes.PAYMENT_FAILED, id, new Payloads.PaymentFailed(id.toString(), "p-2", "card_declined"));

        assertThat(orders.find(id).orElseThrow().status()).isEqualTo(OrderStatus.CONFIRMED);
        assertThat(outboxTypes()).containsExactly(EventTypes.ORDER_CREATED, EventTypes.ORDER_CONFIRMED);
    }

    @Test
    void readingAnOrderNeedsOnlyOneConnectionAtATime() {
        // Regression: find() used to query items/timeline inside the row mapper, holding one pooled
        // connection while borrowing another. Under load (50 concurrent GETs, pool of 20) that
        // deadlocked the pool. With a pool of exactly 1 the old code fails here after 2 s.
        try (TestDatabase single = TestDatabase.h2WithPool(1, "classpath:db/orders")) {
            var svc = new OrderService(single.jdbc, single.tx, new OutboxWriter(single.jdbc, codec), Clock.systemUTC());
            UUID id = svc.placeOrder(request(null), null).orderId();
            var view = svc.find(id).orElseThrow();
            assertThat(view.items()).hasSize(2);
            assertThat(view.timeline()).hasSize(1);
        }
    }

    @Test
    void stateMachineAllowsOnlyPendingToTerminal() {
        assertThat(OrderStatus.PENDING.canTransitionTo(OrderStatus.CONFIRMED)).isTrue();
        assertThat(OrderStatus.PENDING.canTransitionTo(OrderStatus.CANCELLED)).isTrue();
        assertThat(OrderStatus.CONFIRMED.canTransitionTo(OrderStatus.CANCELLED)).isFalse();
        assertThat(OrderStatus.CANCELLED.canTransitionTo(OrderStatus.CONFIRMED)).isFalse();
        assertThat(OrderStatus.PENDING.canTransitionTo(OrderStatus.PENDING)).isFalse();
    }
}
