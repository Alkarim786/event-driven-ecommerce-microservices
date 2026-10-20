package io.github.alkarim786.shop.notification;

import io.github.alkarim786.shop.events.EventCodec;
import io.github.alkarim786.shop.events.EventEnvelope;
import io.github.alkarim786.shop.events.EventTypes;
import io.github.alkarim786.shop.events.Payloads;
import io.github.alkarim786.shop.messaging.IdempotentConsumer;
import io.github.alkarim786.shop.messaging.testing.TestDatabase;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Clock;

import static org.assertj.core.api.Assertions.assertThat;

class NotificationHandlerTest {

    private final TestDatabase db = TestDatabase.h2("classpath:db/notifications");
    private final EventCodec codec = new EventCodec();
    private final NotificationHandler handler = new NotificationHandler(db.jdbc, codec, Clock.systemUTC());
    private final IdempotentConsumer consumer = new IdempotentConsumer(db.jdbc, db.tx, Clock.systemUTC(), 5);

    @AfterEach
    void close() {
        db.close();
    }

    private void deliver(EventEnvelope env) {
        consumer.process(NotificationHandler.CONSUMER_GROUP, env, handler::handle);
    }

    @Test
    void oneEmailPerTerminalEventEvenWhenRedelivered() {
        EventEnvelope confirmed = codec.envelope(EventTypes.ORDER_CONFIRMED, "o-1",
                new Payloads.OrderConfirmed("o-1", "cust", 129900));
        deliver(confirmed);
        deliver(confirmed);

        assertThat(db.count("SELECT COUNT(*) FROM notifications")).isEqualTo(1);
        assertThat(db.jdbc.queryForObject("SELECT message FROM notifications", String.class))
                .contains("confirmed").contains("1299.00");
    }

    @Test
    void cancellationExplainsTheReason() {
        deliver(codec.envelope(EventTypes.ORDER_CANCELLED, "o-2",
                new Payloads.OrderCancelled("o-2", "cust", "payment failed: card_declined")));
        assertThat(db.jdbc.queryForObject("SELECT message FROM notifications", String.class))
                .contains("card_declined").contains("not been charged");
    }

    @Test
    void nonTerminalEventsAreIgnored() {
        deliver(codec.envelope(EventTypes.ORDER_CREATED, "o-3", new Payloads.OrderCreated("o-3", "c", java.util.List.of(), 1, "t")));
        assertThat(db.count("SELECT COUNT(*) FROM notifications")).isZero();
    }
}
