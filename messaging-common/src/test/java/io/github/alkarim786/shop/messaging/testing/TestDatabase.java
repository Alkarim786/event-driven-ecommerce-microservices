package io.github.alkarim786.shop.messaging.testing;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.flywaydb.core.Flyway;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.transaction.support.TransactionTemplate;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Embedded H2 database in PostgreSQL mode with the real Flyway migrations applied. Used by the
 * fast unit tests so they need neither Docker nor Postgres; the Testcontainers suites cover the
 * Postgres-specific behaviour (SKIP LOCKED under real concurrency, partial index).
 */
public final class TestDatabase implements AutoCloseable {

    public final HikariDataSource dataSource;
    public final JdbcTemplate jdbc;
    public final TransactionTemplate tx;

    private TestDatabase(HikariDataSource ds) {
        this.dataSource = ds;
        this.jdbc = new JdbcTemplate(ds);
        this.tx = new TransactionTemplate(new DataSourceTransactionManager(ds));
    }

    /** Fresh, isolated database with the shared messaging tables plus the given migration locations. */
    public static TestDatabase h2(String... extraLocations) {
        return h2WithPool(16, extraLocations);
    }

    /** Same, with a fixed pool size (a pool of 1 exposes code that needs two connections at once). */
    public static TestDatabase h2WithPool(int poolSize, String... extraLocations) {
        String url = "jdbc:h2:mem:" + UUID.randomUUID()
                + ";MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DEFAULT_NULL_ORDERING=HIGH;DB_CLOSE_DELAY=-1;LOCK_TIMEOUT=10000";
        // Flyway needs two connections at once, so migrate outside the (possibly tiny) pool.
        migrate(new DriverManagerDataSource(url), extraLocations);
        HikariConfig cfg = new HikariConfig();
        cfg.setJdbcUrl(url);
        cfg.setMaximumPoolSize(poolSize);
        cfg.setConnectionTimeout(2_000);
        return new TestDatabase(new HikariDataSource(cfg));
    }

    private static void migrate(DataSource ds, String... extraLocations) {
        List<String> locations = new ArrayList<>(List.of("classpath:db/messaging"));
        locations.addAll(List.of(extraLocations));
        Flyway.configure().dataSource(ds).locations(locations.toArray(String[]::new)).load().migrate();
    }

    public long count(String sql, Object... args) {
        Long n = jdbc.queryForObject(sql, Long.class, args);
        return n == null ? 0 : n;
    }

    @Override
    public void close() {
        dataSource.close();
    }
}
