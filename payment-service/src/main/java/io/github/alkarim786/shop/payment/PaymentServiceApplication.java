package io.github.alkarim786.shop.payment;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;

@SpringBootApplication
public class PaymentServiceApplication {

    /** Config lives in payment-service.yml (not application.yml) so all services can boot in one JVM in system tests. */
    public static SpringApplicationBuilder builder() {
        return new SpringApplicationBuilder(PaymentServiceApplication.class).properties("spring.config.name=payment-service");
    }

    public static void main(String[] args) {
        builder().run(args);
    }
}
