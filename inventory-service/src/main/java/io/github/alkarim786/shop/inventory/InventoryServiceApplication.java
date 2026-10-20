package io.github.alkarim786.shop.inventory;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;

@SpringBootApplication
public class InventoryServiceApplication {

    /** Config lives in inventory-service.yml (not application.yml) so all services can boot in one JVM in system tests. */
    public static SpringApplicationBuilder builder() {
        return new SpringApplicationBuilder(InventoryServiceApplication.class).properties("spring.config.name=inventory-service");
    }

    public static void main(String[] args) {
        builder().run(args);
    }
}
