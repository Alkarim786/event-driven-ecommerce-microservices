package io.github.alkarim786.shop.system;

import io.github.alkarim786.shop.events.Topics;
import io.github.alkarim786.shop.inventory.InventoryServiceApplication;
import io.github.alkarim786.shop.messaging.OutboxRelay;
import io.github.alkarim786.shop.notification.NotificationServiceApplication;
import io.github.alkarim786.shop.order.OrderServiceApplication;
import io.github.alkarim786.shop.payment.PaymentServiceApplication;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.web.client.RestClient;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.MountableFile;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * All four Spring Boot services in one JVM against REAL Kafka (KRaft) and Postgres in Docker.
 * The relays run with a 50% chaos duplicate rate, so every scenario also proves that consumers
 * are idempotent under redelivery.
 */
@Tag("integration")
class KafkaSagaIT {

    static final KafkaContainer kafka = new KafkaContainer("apache/kafka:3.9.1");
    static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine")
            .withUsername("shop").withPassword("shop").withDatabaseName("shop")
            .withCopyFileToContainer(MountableFile.forHostPath("../docker/postgres-init.sql"),
                    "/docker-entrypoint-initdb.d/init.sql");

    static final List<ConfigurableApplicationContext> apps = new ArrayList<>();
    static RestClient orders;
    static RestClient inventory;
    static RestClient payments;
    static RestClient notifications;

    @BeforeAll
    static void startStack() {
        kafka.start();
        postgres.start();
        orders = client(boot(OrderServiceApplication.builder(), "orders"));
        inventory = client(boot(InventoryServiceApplication.builder(), "inventory"));
        payments = client(boot(PaymentServiceApplication.builder(), "payments"));
        notifications = client(boot(NotificationServiceApplication.builder(), "notifications"));

        // Test-only SKUs so scenarios don't interfere through shared stock.
        jdbc("orders").update("INSERT INTO catalog (sku, name, price_cents) VALUES ('IT-RACE', 'race', 1000), ('IT-COMP', 'comp', 1000)");
        jdbc("inventory").update("INSERT INTO products (sku, name, available) VALUES ('IT-RACE', 'race', 5), ('IT-COMP', 'comp', 10)");
    }

    @AfterAll
    static void stopStack() {
        apps.forEach(ConfigurableApplicationContext::close);
        kafka.stop();
        postgres.stop();
    }

    private static ConfigurableApplicationContext boot(SpringApplicationBuilder builder, String db) {
        ConfigurableApplicationContext ctx = builder.run(
                "--server.port=0",
                "--spring.datasource.url=" + dbUrl(db),
                "--spring.kafka.bootstrap-servers=" + kafka.getBootstrapServers(),
                "--shop.outbox.poll-interval=50ms",
                "--shop.outbox.chaos-duplicate-rate=0.5",
                "--logging.level.org.apache.kafka=WARN");
        apps.add(ctx);
        return ctx;
    }

    private static String dbUrl(String db) {
        return "jdbc:postgresql://" + postgres.getHost() + ":" + postgres.getMappedPort(5432) + "/" + db;
    }

    private static JdbcTemplate jdbc(String db) {
        return new JdbcTemplate(new DriverManagerDataSource(dbUrl(db), "shop", "shop"));
    }

    private static RestClient client(ConfigurableApplicationContext ctx) {
        return RestClient.builder().baseUrl("http://localhost:" + ctx.getEnvironment().getProperty("local.server.port")).build();
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> placeOrder(String token, String idempotencyKey, Map<String, Object>... items) {
        var req = orders.post().uri("/orders").contentType(MediaType.APPLICATION_JSON)
                .body(Map.of("customerId", "it-customer", "items", List.of(items), "paymentToken", token));
        if (idempotencyKey != null) {
            req = req.header("Idempotency-Key", idempotencyKey);
        }
        return req.retrieve().body(Map.class);
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> awaitTerminal(String orderId) {
        return await().atMost(Duration.ofSeconds(60)).pollInterval(Duration.ofMillis(100))
                .until(() -> orders.get().uri("/orders/{id}", orderId).retrieve().body(Map.class),
                        o -> !"PENDING".equals(o.get("status")));
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> product(String sku) {
        return inventory.get().uri("/inventory/{sku}", sku).retrieve().body(Map.class);
    }

    @Test
    @SuppressWarnings("unchecked")
    void happyPathOverRealKafkaAndPostgres() {
        String id = (String) placeOrder("tok_ok", null, Map.of("sku", "MOUSE-02", "quantity", 2)).get("orderId");

        Map<String, Object> order = awaitTerminal(id);
        assertThat(order.get("status")).isEqualTo("CONFIRMED");
        assertThat((List<Map<String, Object>>) order.get("timeline")).extracting(e -> e.get("event"))
                .contains("OrderCreated", "StockReserved", "PaymentCompleted", "OrderConfirmed");
        assertThat(payments.get().uri("/payments/{id}", id).retrieve().body(Map.class).get("status"))
                .isEqualTo("COMPLETED");
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(notifications.get().uri("/notifications?orderId={id}", id).retrieve().body(List.class))
                        .hasSize(1));
    }

    @Test
    void paymentDeclineCompensatesInventory() {
        String id = (String) placeOrder("tok_decline", null, Map.of("sku", "IT-COMP", "quantity", 4)).get("orderId");

        assertThat(awaitTerminal(id).get("status")).isEqualTo("CANCELLED");
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(product("IT-COMP")).containsEntry("available", 10).containsEntry("reserved", 0));
    }

    @Test
    void concurrentBuyersForLastUnitsNeverOversell() throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(10);
        List<Future<Map<String, Object>>> placed = new ArrayList<>();
        try {
            for (int i = 0; i < 20; i++) {
                placed.add(pool.submit(() -> placeOrder("tok_ok", null, Map.of("sku", "IT-RACE", "quantity", 1))));
            }
            long confirmed = 0;
            for (var f : placed) {
                if ("CONFIRMED".equals(awaitTerminal((String) f.get().get("orderId")).get("status"))) {
                    confirmed++;
                }
            }
            assertThat(confirmed).isEqualTo(5);
        } finally {
            pool.shutdownNow();
        }
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(product("IT-RACE")).containsEntry("available", 0)
                        .containsEntry("reserved", 0).containsEntry("sold", 5));
    }

    @Test
    void idempotencyKeyPreventsDuplicateOrdersOnClientRetry() {
        var first = placeOrder("tok_ok", "retry-key-1", Map.of("sku", "KB-MECH-01", "quantity", 1));
        var retry = placeOrder("tok_ok", "retry-key-1", Map.of("sku", "KB-MECH-01", "quantity", 1));
        assertThat(retry.get("orderId")).isEqualTo(first.get("orderId"));
        assertThat(jdbc("orders").queryForObject("SELECT COUNT(*) FROM orders WHERE idempotency_key = 'retry-key-1'",
                Integer.class)).isEqualTo(1);
    }

    @Test
    void poisonMessageGoesToDeadLetterTopicAndDoesNotBlockThePartition() {
        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        try (var producer = new KafkaProducer<String, String>(p)) {
            producer.send(new ProducerRecord<>(Topics.ORDERS, "poison", "{this is not json")).get();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }

        Properties c = new Properties();
        c.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        c.put(ConsumerConfig.GROUP_ID_CONFIG, "it-dlt-reader");
        c.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        c.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        c.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        try (var consumer = new KafkaConsumer<String, String>(c)) {
            consumer.subscribe(List.of(Topics.deadLetter(Topics.ORDERS)));
            List<ConsumerRecord<String, String>> seen = new ArrayList<>();
            await().atMost(Duration.ofSeconds(60)).until(() -> {
                consumer.poll(Duration.ofMillis(500)).forEach(seen::add);
                return seen.stream().anyMatch(r -> "poison".equals(r.key()));
            });
        }
        // The partition keeps flowing after the poison message.
        String id = (String) placeOrder("tok_ok", null, Map.of("sku", "MOUSE-02", "quantity", 1)).get("orderId");
        assertThat(awaitTerminal(id).get("status")).isEqualTo("CONFIRMED");
    }

    @Test
    void chaosDuplicatesWerePublishedYetEveryOrderHasOnePaymentAndOneNotification() {
        for (int i = 0; i < 10; i++) {
            String id = (String) placeOrder(i % 3 == 0 ? "tok_decline" : "tok_ok", null,
                    Map.of("sku", "USB-C-HUB", "quantity", 1)).get("orderId");
            awaitTerminal(id);
        }
        long duplicates = apps.stream().filter(a -> a.getBeanNamesForType(OutboxRelay.class).length > 0)
                .mapToLong(a -> a.getBean(OutboxRelay.class).chaosDuplicates()).sum();
        assertThat(duplicates).isPositive();

        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            assertThat(jdbc("payments").queryForObject(
                    "SELECT COUNT(*) FROM (SELECT order_id FROM payments GROUP BY order_id HAVING COUNT(*) > 1) d",
                    Integer.class)).isZero();
            Integer terminal = jdbc("orders").queryForObject("SELECT COUNT(*) FROM orders WHERE status <> 'PENDING'",
                    Integer.class);
            assertThat(jdbc("notifications").queryForObject("SELECT COUNT(*) FROM notifications", Integer.class))
                    .isEqualTo(terminal);
        });
    }
}
