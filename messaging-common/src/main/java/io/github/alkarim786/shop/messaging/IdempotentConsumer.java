package io.github.alkarim786.shop.messaging;

import io.github.alkarim786.shop.events.EventEnvelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.ConcurrencyFailureException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionTemplate;

import java.sql.Timestamp;
import java.time.Clock;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;

/**
 * Exactly-once <i>effect</i> on top of at-least-once delivery.
 *
 * <p>In ONE local transaction: insert (consumerGroup, eventId) into processed_messages, then run
 * the side effect (which typically also appends follow-up events to the outbox). A redelivered
 * event violates the primary key, the transaction rolls back, and the effect is skipped. Two
 * concurrent deliveries of the same event serialise on the unique index: the second blocks until
 * the first commits, then fails with a duplicate key.
 *
 * <p>Optimistic-lock conflicts inside the effect (e.g. two orders racing for the last unit) roll
 * back the whole transaction, including the processed_messages row, and are retried here with
 * jittered backoff. If retries run out the exception propagates to the Kafka error handler, which
 * redelivers and finally dead-letters.
 */
public class IdempotentConsumer {

    private static final Logger log = LoggerFactory.getLogger(IdempotentConsumer.class);

    private final JdbcTemplate jdbc;
    private final TransactionTemplate tx;
    private final Clock clock;
    private final int maxAttempts;

    public IdempotentConsumer(JdbcTemplate jdbc, TransactionTemplate tx, Clock clock, int maxAttempts) {
        this.jdbc = jdbc;
        this.tx = tx;
        this.clock = clock;
        this.maxAttempts = maxAttempts;
    }

    public enum Outcome { PROCESSED, DUPLICATE }

    public Outcome process(String consumerGroup, EventEnvelope event, Consumer<EventEnvelope> effect) {
        for (int attempt = 1; ; attempt++) {
            try {
                tx.executeWithoutResult(status -> {
                    markProcessed(consumerGroup, event);
                    effect.accept(event);
                });
                return Outcome.PROCESSED;
            } catch (AlreadyProcessed e) {
                log.debug("skip duplicate {} {} for {}", event.type(), event.eventId(), consumerGroup);
                return Outcome.DUPLICATE;
            } catch (ConcurrencyFailureException e) {
                if (attempt >= maxAttempts) {
                    throw e;
                }
                backoff(attempt);
            }
        }
    }

    private void markProcessed(String consumerGroup, EventEnvelope event) {
        try {
            jdbc.update("INSERT INTO processed_messages (consumer_group, event_id, processed_at) VALUES (?, ?, ?)",
                    consumerGroup, event.eventId(), Timestamp.from(clock.instant()));
        } catch (DuplicateKeyException e) {
            // Only a duplicate on THIS insert means "already processed". A unique violation raised
            // by the effect itself is a real error and must not be swallowed.
            throw new AlreadyProcessed();
        }
    }

    private static void backoff(int attempt) {
        long maxMillis = Math.min(50, 2L << attempt);
        try {
            Thread.sleep(ThreadLocalRandom.current().nextLong(1, maxMillis + 1));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted during retry backoff", e);
        }
    }

    /** Thrown inside the transaction to roll it back when the event was seen before. */
    static final class AlreadyProcessed extends RuntimeException {
        AlreadyProcessed() {
            super(null, null, false, false);
        }
    }
}
