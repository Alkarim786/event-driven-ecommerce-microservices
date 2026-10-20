package io.github.alkarim786.shop.events;

import java.util.List;

/** Kafka topics. Each service publishes only to its own topic; all are keyed by orderId. */
public final class Topics {
    private Topics() {}

    public static final String ORDERS = "orders.events";
    public static final String INVENTORY = "inventory.events";
    public static final String PAYMENTS = "payments.events";

    public static final List<String> ALL = List.of(ORDERS, INVENTORY, PAYMENTS);

    /** Dead-letter topic name used by the consumer error handler for poison messages. */
    public static String deadLetter(String topic) {
        return topic + ".DLT";
    }
}
