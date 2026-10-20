package io.github.alkarim786.shop.events;

import java.util.List;

/**
 * Payload records for each event type.
 *
 * <p>Events carry the state downstream services need ("event-carried state transfer"), e.g.
 * StockReserved repeats the amount and payment token from OrderCreated. Payment-service consumes
 * only inventory.events, and Kafka gives no ordering across topics, so it must never have to wait
 * for an OrderCreated that might arrive later.
 */
public final class Payloads {
    private Payloads() {}

    public record LineItem(String sku, int quantity) {}

    public record OrderCreated(String orderId, String customerId, List<LineItem> items,
                               long totalCents, String paymentToken) {}

    public record OrderConfirmed(String orderId, String customerId, long totalCents) {}

    public record OrderCancelled(String orderId, String customerId, String reason) {}

    public record StockReserved(String orderId, String customerId, List<LineItem> items,
                                long totalCents, String paymentToken) {}

    public record StockRejected(String orderId, String reason) {}

    public record StockReleased(String orderId, List<LineItem> items) {}

    public record StockCommitted(String orderId, List<LineItem> items) {}

    public record PaymentCompleted(String orderId, String paymentId, long amountCents) {}

    public record PaymentFailed(String orderId, String paymentId, String reason) {}
}
