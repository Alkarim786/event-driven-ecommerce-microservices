package io.github.alkarim786.shop.messaging;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

import java.time.Duration;

/**
 * Relay tuning, bound from {@code shop.outbox.*}.
 *
 * @param lockClause         row-locking clause appended to the poll query; Postgres uses
 *                           {@code FOR UPDATE SKIP LOCKED}
 * @param chaosDuplicateRate probability [0,1] of publishing an event twice (fault injection used
 *                           by tests and the benchmark to prove consumers are idempotent)
 */
@ConfigurationProperties("shop.outbox")
public record OutboxProperties(
        @DefaultValue("true") boolean enabled,
        @DefaultValue("200") int batchSize,
        @DefaultValue("100ms") Duration pollInterval,
        @DefaultValue("1s") Duration errorBackoff,
        @DefaultValue("10s") Duration sendTimeout,
        @DefaultValue("1h") Duration retention,
        @DefaultValue("FOR UPDATE SKIP LOCKED") String lockClause,
        @DefaultValue("0") double chaosDuplicateRate) {

    public static OutboxProperties defaults() {
        return new OutboxProperties(true, 200, Duration.ofMillis(100), Duration.ofSeconds(1),
                Duration.ofSeconds(10), Duration.ofHours(1), "FOR UPDATE SKIP LOCKED", 0);
    }

    public OutboxProperties withChaosDuplicateRate(double rate) {
        return new OutboxProperties(enabled, batchSize, pollInterval, errorBackoff, sendTimeout,
                retention, lockClause, rate);
    }
}
