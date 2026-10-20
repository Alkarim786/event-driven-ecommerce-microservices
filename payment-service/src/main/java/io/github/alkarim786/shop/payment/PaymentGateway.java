package io.github.alkarim786.shop.payment;

/**
 * Port to an external card processor. {@code idempotencyKey} is the order id, so a retried charge
 * (after a crash between the gateway call and our commit) is de-duplicated by the gateway itself,
 * the same way Stripe's Idempotency-Key header works.
 */
public interface PaymentGateway {

    record ChargeResult(boolean approved, String declineReason) {
        static ChargeResult ok() {
            return new ChargeResult(true, null);
        }

        static ChargeResult declined(String reason) {
            return new ChargeResult(false, reason);
        }
    }

    ChargeResult charge(String idempotencyKey, String customerId, long amountCents, String token);
}
