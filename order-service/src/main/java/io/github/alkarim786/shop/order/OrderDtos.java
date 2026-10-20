package io.github.alkarim786.shop.order;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.Size;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

public final class OrderDtos {
    private OrderDtos() {}

    public record ItemRequest(@NotBlank String sku, @Min(1) @Max(100) int quantity) {}

    /**
     * @param paymentToken           simulated card token: {@code tok_ok} (default), {@code tok_decline},
     *                               {@code tok_insufficient_funds}
     * @param simulatePaymentFailure shortcut for {@code paymentToken = tok_decline}
     */
    public record PlaceOrderRequest(
            @NotBlank @Size(max = 64) String customerId,
            @NotEmpty @Size(max = 20) List<@Valid ItemRequest> items,
            @Size(max = 64) String paymentToken,
            Boolean simulatePaymentFailure) {

        String effectiveToken() {
            if (Boolean.TRUE.equals(simulatePaymentFailure)) {
                return "tok_decline";
            }
            return paymentToken == null || paymentToken.isBlank() ? "tok_ok" : paymentToken;
        }
    }

    public record PlaceOrderResponse(UUID orderId, OrderStatus status) {}

    public record ItemView(String sku, int quantity, long unitPriceCents) {}

    public record TimelineEntry(String event, String detail, Instant at) {}

    public record OrderView(UUID orderId, String customerId, OrderStatus status, long totalCents,
                            String cancelReason, Instant createdAt, Instant completedAt,
                            Long sagaLatencyMs, List<ItemView> items, List<TimelineEntry> timeline) {}
}
