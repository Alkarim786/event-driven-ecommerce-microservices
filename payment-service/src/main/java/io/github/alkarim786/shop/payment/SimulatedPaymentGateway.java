package io.github.alkarim786.shop.payment;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Deterministic test-mode processor, modelled on Stripe test tokens:
 * {@code tok_decline} → card_declined, {@code tok_insufficient_funds} → insufficient_funds,
 * and any amount above the per-charge limit → amount_exceeds_limit. No real money moves.
 */
@Component
public class SimulatedPaymentGateway implements PaymentGateway {

    private final long maxChargeCents;
    private final long latencyMs;

    public SimulatedPaymentGateway(@Value("${shop.payment.max-charge-cents:50000000}") long maxChargeCents,
                                   @Value("${shop.payment.latency-ms:0}") long latencyMs) {
        this.maxChargeCents = maxChargeCents;
        this.latencyMs = latencyMs;
    }

    @Override
    public ChargeResult charge(String idempotencyKey, String customerId, long amountCents, String token) {
        if (latencyMs > 0) {
            try {
                Thread.sleep(latencyMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("interrupted", e);
            }
        }
        if ("tok_decline".equals(token)) {
            return ChargeResult.declined("card_declined");
        }
        if ("tok_insufficient_funds".equals(token)) {
            return ChargeResult.declined("insufficient_funds");
        }
        if (amountCents > maxChargeCents) {
            return ChargeResult.declined("amount_exceeds_limit");
        }
        return ChargeResult.ok();
    }
}
