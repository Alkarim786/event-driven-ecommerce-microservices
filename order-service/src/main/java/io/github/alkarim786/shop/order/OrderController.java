package io.github.alkarim786.shop.order;

import io.github.alkarim786.shop.order.OrderDtos.OrderView;
import io.github.alkarim786.shop.order.OrderDtos.PlaceOrderRequest;
import io.github.alkarim786.shop.order.OrderDtos.PlaceOrderResponse;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

@RestController
@RequestMapping("/orders")
class OrderController {

    private final OrderService orders;

    OrderController(OrderService orders) {
        this.orders = orders;
    }

    /** 202 Accepted: the order exists, but its outcome is decided asynchronously by the saga. */
    @PostMapping
    ResponseEntity<PlaceOrderResponse> place(@Valid @RequestBody PlaceOrderRequest request,
                                             @RequestHeader(value = "Idempotency-Key", required = false) String key) {
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(orders.placeOrder(request, key));
    }

    @GetMapping("/{id}")
    ResponseEntity<OrderView> get(@PathVariable UUID id) {
        return ResponseEntity.of(orders.find(id));
    }

    @ExceptionHandler(OrderService.UnknownSkuException.class)
    ProblemDetail unknownSku(OrderService.UnknownSkuException e) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, e.getMessage());
    }
}
