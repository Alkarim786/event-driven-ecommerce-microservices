package io.github.alkarim786.shop.notification;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;

@SpringBootApplication
public class NotificationServiceApplication {

    /** Config lives in notification-service.yml (not application.yml) so all services can boot in one JVM in system tests. */
    public static SpringApplicationBuilder builder() {
        return new SpringApplicationBuilder(NotificationServiceApplication.class).properties("spring.config.name=notification-service");
    }

    public static void main(String[] args) {
        builder().run(args);
    }
}
