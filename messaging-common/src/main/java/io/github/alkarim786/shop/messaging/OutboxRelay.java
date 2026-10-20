package io.github.alkarim786.shop.messaging;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionTemplate;

import java.sql.Timestamp;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Polling publisher for the transactional outbox.
 *
 * <p>Each pass, in one DB transaction: lock up to {@code batchSize} unpublished rows in
 * {@code seq} order with {@code FOR UPDATE SKIP LOCKED}, send them all to the broker, wait for
 * every ack, then mark them published. If any send fails the transaction rolls back and the whole
 * batch is retried on the next pass. Rows that were already acked get sent again, so delivery is
 * <b>at-least-once</b>; consumers make that safe with {@link IdempotentConsumer}.
 *
 * <p>SKIP LOCKED lets several relay instances share one table without blocking each other. Note
 * the ordering caveat: two instances could then publish events of the same order out of order, so
 * production runs one relay per service and scales out by partitioning, not by racing.
 */
public class OutboxRelay implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);

    private final JdbcTemplate jdbc;
    private final TransactionTemplate tx;
    private final EventPublisher publisher;
    private final OutboxProperties props;
    private final Clock clock;
    private final String selectSql;

    // Capacity 1: a commit that lands while a pass is running leaves one pending signal, so it is never lost.
    private final ArrayBlockingQueue<Boolean> wakeUps = new ArrayBlockingQueue<>(1);
    private final AtomicLong publishedTotal = new AtomicLong();
    private final AtomicLong chaosDuplicates = new AtomicLong();
    private volatile Thread worker;
    private volatile boolean running;

    public OutboxRelay(JdbcTemplate jdbc, TransactionTemplate tx, EventPublisher publisher,
                       OutboxProperties props, Clock clock) {
        this.jdbc = jdbc;
        this.tx = tx;
        this.publisher = publisher;
        this.props = props;
        this.clock = clock;
        this.selectSql = """
                SELECT seq, topic, aggregate_id, payload FROM outbox
                WHERE published_at IS NULL ORDER BY seq LIMIT ?
                """ + props.lockClause();
    }

    private record Row(long seq, String topic, String key, String payload) {}

    /** Publishes one batch. Returns how many rows were published. */
    public int relayOnce() {
        Integer n = tx.execute(status -> {
            List<Row> rows = jdbc.query(selectSql,
                    (rs, i) -> new Row(rs.getLong(1), rs.getString(2), rs.getString(3), rs.getString(4)),
                    props.batchSize());
            if (rows.isEmpty()) {
                return 0;
            }
            List<CompletableFuture<Void>> acks = new ArrayList<>(rows.size() + 4);
            for (Row r : rows) {
                acks.add(publisher.publish(r.topic(), r.key(), r.payload()));
                if (props.chaosDuplicateRate() > 0
                        && ThreadLocalRandom.current().nextDouble() < props.chaosDuplicateRate()) {
                    // Fault injection: simulate the "acked but crashed before marking published" case.
                    acks.add(publisher.publish(r.topic(), r.key(), r.payload()));
                    chaosDuplicates.incrementAndGet();
                }
            }
            awaitAll(acks);
            List<Object[]> args = new ArrayList<>(rows.size());
            Timestamp now = Timestamp.from(clock.instant());
            for (Row r : rows) {
                args.add(new Object[]{now, r.seq()});
            }
            jdbc.batchUpdate("UPDATE outbox SET published_at = ? WHERE seq = ?", args);
            return rows.size();
        });
        int published = n == null ? 0 : n;
        publishedTotal.addAndGet(published);
        return published;
    }

    private void awaitAll(List<CompletableFuture<Void>> acks) {
        try {
            CompletableFuture.allOf(acks.toArray(CompletableFuture[]::new))
                    .get(props.sendTimeout().toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new OutboxPublishException("interrupted while waiting for broker acks", e);
        } catch (ExecutionException | TimeoutException e) {
            throw new OutboxPublishException("broker did not ack outbox batch", e);
        }
    }

    /** Deletes rows published more than {@code retention} ago. */
    public int purgePublished(Duration retention) {
        Timestamp cutoff = Timestamp.from(clock.instant().minus(retention));
        return jdbc.update("DELETE FROM outbox WHERE published_at IS NOT NULL AND published_at < ?", cutoff);
    }

    public long pendingCount() {
        Long n = jdbc.queryForObject("SELECT COUNT(*) FROM outbox WHERE published_at IS NULL", Long.class);
        return n == null ? 0 : n;
    }

    public long publishedTotal() {
        return publishedTotal.get();
    }

    public long chaosDuplicates() {
        return chaosDuplicates.get();
    }

    /** Non-blocking hint that new rows were committed; skips the rest of the poll interval. */
    public void wakeUp() {
        wakeUps.offer(Boolean.TRUE);
    }

    private void loop() {
        long lastPurge = System.nanoTime();
        while (running) {
            try {
                int n = relayOnce();
                if (n >= props.batchSize()) {
                    continue; // backlog: drain without sleeping
                }
                if (System.nanoTime() - lastPurge > TimeUnit.MINUTES.toNanos(1)) {
                    purgePublished(props.retention());
                    lastPurge = System.nanoTime();
                }
                wakeUps.poll(props.pollInterval().toMillis(), TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (RuntimeException e) {
                log.warn("outbox relay pass failed, retrying in {}: {}", props.errorBackoff(), e.toString());
                sleepQuietly(props.errorBackoff());
            }
        }
    }

    private static void sleepQuietly(Duration d) {
        try {
            Thread.sleep(d.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void start() {
        running = true;
        worker = Thread.ofPlatform().name("outbox-relay").daemon().start(this::loop);
    }

    @Override
    public void stop() {
        running = false;
        Thread t = worker;
        if (t != null) {
            t.interrupt();
            try {
                t.join(5_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    public static class OutboxPublishException extends RuntimeException {
        public OutboxPublishException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
