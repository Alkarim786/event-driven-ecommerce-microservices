package io.github.alkarim786.shop.events;

/** Event type names. Strings (not an enum) so an unknown type from a newer producer never breaks parsing. */
public final class EventTypes {
    private EventTypes() {}

    // orders.events
    public static final String ORDER_CREATED = "OrderCreated";
    public static final String ORDER_CONFIRMED = "OrderConfirmed";
    public static final String ORDER_CANCELLED = "OrderCancelled";

    // inventory.events
    public static final String STOCK_RESERVED = "StockReserved";
    public static final String STOCK_REJECTED = "StockRejected";
    public static final String STOCK_RELEASED = "StockReleased";
    public static final String STOCK_COMMITTED = "StockCommitted";

    // payments.events
    public static final String PAYMENT_COMPLETED = "PaymentCompleted";
    public static final String PAYMENT_FAILED = "PaymentFailed";
}
