package io.github.alkarim786.shop.messaging;

import io.github.alkarim786.shop.events.EventCodec;
import io.github.alkarim786.shop.events.EventEnvelope;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.sql.Timestamp;

/**
 * Appends events to the outbox table. Must be called inside the same transaction as the business
 * change: that is the whole point of the pattern (the row and the event commit or roll back
 * together), so calling it outside a transaction is a programming error and fails fast.
 */
public class OutboxWriter {

    private final JdbcTemplate jdbc;
    private final EventCodec codec;
    private volatile Runnable afterCommitHook = () -> {};

    public OutboxWriter(JdbcTemplate jdbc, EventCodec codec) {
        this.jdbc = jdbc;
        this.codec = codec;
    }

    /** Called after a transaction that wrote events commits; the relay uses it to wake up early. */
    public void onCommit(Runnable hook) {
        this.afterCommitHook = hook;
    }

    public EventEnvelope append(String topic, String type, String aggregateId, Object payload) {
        if (!TransactionSynchronizationManager.isActualTransactionActive()) {
            throw new IllegalStateException("OutboxWriter.append must run inside a transaction");
        }
        EventEnvelope env = codec.envelope(type, aggregateId, payload);
        jdbc.update("""
                INSERT INTO outbox (event_id, topic, aggregate_id, event_type, payload, created_at)
                VALUES (?, ?, ?, ?, ?, ?)""",
                env.eventId(), topic, aggregateId, type, codec.toJson(env), Timestamp.from(env.occurredAt()));
        registerWakeUp();
        return env;
    }

    private void registerWakeUp() {
        if (!TransactionSynchronizationManager.isSynchronizationActive()) {
            return;
        }
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCommit() {
                afterCommitHook.run();
            }
        });
    }
}
