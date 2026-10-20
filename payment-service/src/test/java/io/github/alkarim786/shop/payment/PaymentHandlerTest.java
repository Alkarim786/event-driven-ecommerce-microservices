package io.github.alkarim786.shop.payment;

import io.github.alkarim786.shop.events.EventCodec;
import io.github.alkarim786.shop.events.EventEnvelope;
import io.github.alkarim786.shop.events.EventTypes;
import io.github.alkarim786.shop.events.Payloads;
import io.github.alkarim786.shop.messaging.IdempotentConsumer;
import io.github.alkarim786.shop.messaging.OutboxWriter;
import io.github.alkarim786.shop.messaging.testing.TestDatabase;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class PaymentHandlerTest {

    private final TestDatabase db = TestDatabase.h2("classpath:db/payments");
    private final EventCodec codec = new EventCodec();
    private final AtomicInteger gatewayCalls = new AtomicInteger();
    private final SimulatedPaymentGateway simulated = new SimulatedPaymentGateway(1_000_000, 0);
    private final PaymentGateway countingGateway = (key, customer, amount, token) -> {
        gatewayCalls.incrementAndGet();
        return simulated.charge(key, customer, amount, token);
    };
    private final PaymentHandler handler = new PaymentHandler(db.jdbc, countingGateway,
            new OutboxWriter(db.jdbc, codec), codec, Clock.systemUTC());
    private final IdempotentConsumer consumer = new IdempotentConsumer(db.jdbc, db.tx, Clock.systemUTC(), 5);

    @AfterEach
    void close() {
        db.close();
    }

    private EventEnvelope stockReserved(String orderId, long amount, String token) {
        return codec.envelope(EventTypes.STOCK_RESERVED, orderId,
                new Payloads.StockReserved(orderId, "cust", List.of(), amount, token));
    }

    private void deliver(EventEnvelope env) {
        consumer.process(PaymentHandler.CONSUMER_GROUP, env, handler::handle);
    }

    private String status(String orderId) {
        return db.jdbc.queryForObject("SELECT status FROM payments WHERE order_id = ?", String.class, orderId);
    }

    private List<String> outboxTypes() {
        return db.jdbc.queryForList("SELECT event_type FROM outbox ORDER BY seq", String.class);
    }

    @Test
    void approvedChargeEmitsPaymentCompleted() {
        deliver(stockReserved("o-1", 5000, "tok_ok"));
        assertThat(status("o-1")).isEqualTo("COMPLETED");
        assertThat(outboxTypes()).containsExactly(EventTypes.PAYMENT_COMPLETED);
    }

    @Test
    void declinedTokenEmitsPaymentFailedWithReason() {
        deliver(stockReserved("o-1", 5000, "tok_decline"));
        assertThat(status("o-1")).isEqualTo("FAILED");
        var failed = codec.payload(codec.parse(db.jdbc.queryForObject("SELECT payload FROM outbox", String.class)),
                Payloads.PaymentFailed.class);
        assertThat(failed.reason()).isEqualTo("card_declined");
    }

    @Test
    void amountAboveLimitIsDeclined() {
        deliver(stockReserved("o-1", 1_000_001, "tok_ok"));
        assertThat(status("o-1")).isEqualTo("FAILED");
    }

    @Test
    void redeliveredAndReEmittedStockReservedChargeOnlyOnce() {
        EventEnvelope env = stockReserved("o-1", 5000, "tok_ok");
        deliver(env);
        deliver(env);                                // same event id: processed_messages skip
        deliver(stockReserved("o-1", 5000, "tok_ok")); // new event id, same order: business-level guard
        assertThat(gatewayCalls).hasValue(1);
        assertThat(db.count("SELECT COUNT(*) FROM payments")).isEqualTo(1);
        assertThat(outboxTypes()).containsExactly(EventTypes.PAYMENT_COMPLETED);
    }

    @Test
    void otherEventTypesAreIgnored() {
        deliver(codec.envelope(EventTypes.STOCK_REJECTED, "o-1", new Payloads.StockRejected("o-1", "x")));
        assertThat(gatewayCalls).hasValue(0);
    }
}
