package io.github.alkarim786.shop.system;

import io.github.alkarim786.shop.events.EventCodec;
import io.github.alkarim786.shop.events.EventEnvelope;
import io.github.alkarim786.shop.events.Topics;
import io.github.alkarim786.shop.inventory.InventoryHandler;
import io.github.alkarim786.shop.messaging.IdempotentConsumer;
import io.github.alkarim786.shop.messaging.OutboxProperties;
import io.github.alkarim786.shop.messaging.OutboxRelay;
import io.github.alkarim786.shop.messaging.OutboxWriter;
import io.github.alkarim786.shop.messaging.testing.RecordingPublisher;
import io.github.alkarim786.shop.messaging.testing.TestDatabase;
import io.github.alkarim786.shop.notification.NotificationHandler;
import io.github.alkarim786.shop.order.OrderSagaHandler;
import io.github.alkarim786.shop.order.OrderService;
import io.github.alkarim786.shop.payment.PaymentHandler;
import io.github.alkarim786.shop.payment.SimulatedPaymentGateway;

import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * The whole shop in one process, with no Kafka and no Postgres: the four services' REAL handlers,
 * outbox writers, relays and idempotent consumers, each on its own H2 database, connected by an
 * in-memory bus. Only the transport is fake, so the saga logic under test is the production code.
 */
final class InMemoryShop implements AutoCloseable {

    final EventCodec codec = new EventCodec();
    final Clock clock = Clock.systemUTC();
    final RecordingPublisher bus = new RecordingPublisher();

    final TestDatabase ordersDb = TestDatabase.h2("classpath:db/orders");
    final TestDatabase inventoryDb = TestDatabase.h2("classpath:db/inventory");
    final TestDatabase paymentsDb = TestDatabase.h2("classpath:db/payments");
    final TestDatabase notificationsDb = TestDatabase.h2("classpath:db/notifications");

    final OrderService orders;
    final List<OutboxRelay> relays = new ArrayList<>();
    private final Map<String, List<Subscriber>> subscribers;
    private final int deliveriesPerMessage;

    record Subscriber(String group, IdempotentConsumer consumer, Consumer<EventEnvelope> handler) {
        void deliver(EventEnvelope env) {
            consumer.process(group, env, handler);
        }
    }

    /**
     * @param deliveriesPerMessage 1 for normal at-least-once delivery, 2+ to deliver every message
     *                             several times (simulated redelivery storm)
     */
    InMemoryShop(int deliveriesPerMessage, double relayChaosDuplicateRate) {
        this.deliveriesPerMessage = deliveriesPerMessage;
        OutboxProperties props = OutboxProperties.defaults().withChaosDuplicateRate(relayChaosDuplicateRate);

        OutboxWriter orderOutbox = new OutboxWriter(ordersDb.jdbc, codec);
        orders = new OrderService(ordersDb.jdbc, ordersDb.tx, orderOutbox, clock);
        var orderSaga = new OrderSagaHandler(ordersDb.jdbc, orders, orderOutbox, codec, clock);

        OutboxWriter inventoryOutbox = new OutboxWriter(inventoryDb.jdbc, codec);
        var inventory = new InventoryHandler(inventoryDb.jdbc, inventoryOutbox, codec, clock);

        OutboxWriter paymentOutbox = new OutboxWriter(paymentsDb.jdbc, codec);
        var payment = new PaymentHandler(paymentsDb.jdbc, new SimulatedPaymentGateway(50_000_000, 0),
                paymentOutbox, codec, clock);

        var notification = new NotificationHandler(notificationsDb.jdbc, codec, clock);

        relays.add(new OutboxRelay(ordersDb.jdbc, ordersDb.tx, bus, props, clock));
        relays.add(new OutboxRelay(inventoryDb.jdbc, inventoryDb.tx, bus, props, clock));
        relays.add(new OutboxRelay(paymentsDb.jdbc, paymentsDb.tx, bus, props, clock));

        var orderSub = new Subscriber(OrderSagaHandler.CONSUMER_GROUP, consumer(ordersDb), orderSaga::handle);
        var inventorySub = new Subscriber(InventoryHandler.CONSUMER_GROUP, consumer(inventoryDb), inventory::handle);
        var paymentSub = new Subscriber(PaymentHandler.CONSUMER_GROUP, consumer(paymentsDb), payment::handle);
        var notificationSub = new Subscriber(NotificationHandler.CONSUMER_GROUP, consumer(notificationsDb),
                notification::handle);

        // Same subscriptions as the @KafkaListener annotations.
        subscribers = Map.of(
                Topics.ORDERS, List.of(inventorySub, notificationSub),
                Topics.INVENTORY, List.of(orderSub, paymentSub),
                Topics.PAYMENTS, List.of(orderSub, inventorySub));
    }

    private IdempotentConsumer consumer(TestDatabase db) {
        return new IdempotentConsumer(db.jdbc, db.tx, clock, 50);
    }

    List<Subscriber> subscribers(String topic) {
        return subscribers.get(topic);
    }

    /** Relays every outbox to the bus. Returns the messages that were published. */
    List<RecordingPublisher.Message> relayAll() {
        for (OutboxRelay r : relays) {
            while (r.relayOnce() > 0) {
                // drain
            }
        }
        return bus.drain();
    }

    /** Runs relays and deliveries until the system is quiet (no unpublished events, no messages). */
    int runUntilQuiet() {
        int delivered = 0;
        for (int round = 0; round < 100; round++) {
            List<RecordingPublisher.Message> batch = relayAll();
            if (batch.isEmpty()) {
                return delivered;
            }
            for (RecordingPublisher.Message m : batch) {
                EventEnvelope env = codec.parse(m.value());
                for (int i = 0; i < deliveriesPerMessage; i++) {
                    for (Subscriber s : subscribers.get(m.topic())) {
                        s.deliver(env);
                        delivered++;
                    }
                }
            }
        }
        throw new IllegalStateException("saga did not settle in 100 rounds");
    }

    long chaosDuplicates() {
        return relays.stream().mapToLong(OutboxRelay::chaosDuplicates).sum();
    }

    @Override
    public void close() {
        ordersDb.close();
        inventoryDb.close();
        paymentsDb.close();
        notificationsDb.close();
    }
}
