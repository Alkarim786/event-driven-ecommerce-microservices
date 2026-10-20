package io.github.alkarim786.shop.order;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;

@SpringBootApplication
public class OrderServiceApplication {

    /** Config lives in order-service.yml (not application.yml) so all services can boot in one JVM in system tests. */
    public static SpringApplicationBuilder builder() {
        return new SpringApplicationBuilder(OrderServiceApplication.class).properties("spring.config.name=order-service");
    }

    public static void main(String[] args) {
        builder().run(args);
    }
}
