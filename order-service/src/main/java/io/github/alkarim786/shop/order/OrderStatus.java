package io.github.alkarim786.shop.order;

/**
 * Order state machine: PENDING → CONFIRMED | CANCELLED. Both outcomes are terminal; an event that
 * arrives for a terminal order (e.g. a late duplicate) is recorded in the timeline and ignored.
 */
public enum OrderStatus {
    PENDING,
    CONFIRMED,
    CANCELLED;

    public boolean isTerminal() {
        return this != PENDING;
    }

    public boolean canTransitionTo(OrderStatus next) {
        return this == PENDING && next.isTerminal();
    }
}
