package io.github.alkarim786.shop.messaging;

import io.github.alkarim786.shop.events.EventCodec;
import io.github.alkarim786.shop.events.EventEnvelope;
import io.github.alkarim786.shop.messaging.testing.TestDatabase;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.dao.OptimisticLockingFailureException;

import java.time.Clock;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class IdempotentConsumerTest {

    private final TestDatabase db = TestDatabase.h2();
    private final IdempotentConsumer consumer = new IdempotentConsumer(db.jdbc, db.tx, Clock.systemUTC(), 5);
    private final EventEnvelope event = new EventCodec().envelope("OrderCreated", "o-1", Map.of());

    @AfterEach
    void close() {
        db.close();
    }

    @Test
    void sameEventTwiceHasExactlyOneEffect() {
        AtomicInteger effects = new AtomicInteger();

        assertThat(consumer.process("inventory", event, e -> effects.incrementAndGet()))
                .isEqualTo(IdempotentConsumer.Outcome.PROCESSED);
        assertThat(consumer.process("inventory", event, e -> effects.incrementAndGet()))
                .isEqualTo(IdempotentConsumer.Outcome.DUPLICATE);

        assertThat(effects).hasValue(1);
    }

    @Test
    void differentConsumerGroupsEachProcessTheEventOnce() {
        consumer.process("inventory", event, e -> {});
        assertThat(consumer.process("payment", event, e -> {})).isEqualTo(IdempotentConsumer.Outcome.PROCESSED);
    }

    @Test
    void failedEffectRollsBackTheMarkerSoRedeliveryIsProcessed() {
        assertThatThrownBy(() -> consumer.process("inventory", event, e -> {
            throw new IllegalStateException("db down");
        })).isInstanceOf(IllegalStateException.class);

        assertThat(db.count("SELECT COUNT(*) FROM processed_messages")).isZero();
        assertThat(consumer.process("inventory", event, e -> {})).isEqualTo(IdempotentConsumer.Outcome.PROCESSED);
    }

    @Test
    void uniqueViolationInsideTheEffectIsARealErrorNotADuplicate() {
        assertThatThrownBy(() -> consumer.process("inventory", event, e -> {
            throw new DuplicateKeyException("payments_order_id_key");
        })).isInstanceOf(DuplicateKeyException.class);
    }

    @Test
    void optimisticLockConflictsAreRetried() {
        AtomicInteger attempts = new AtomicInteger();
        var outcome = consumer.process("inventory", event, e -> {
            if (attempts.incrementAndGet() < 3) {
                throw new OptimisticLockingFailureException("version changed");
            }
        });
        assertThat(outcome).isEqualTo(IdempotentConsumer.Outcome.PROCESSED);
        assertThat(attempts).hasValue(3);
        assertThat(db.count("SELECT COUNT(*) FROM processed_messages")).isEqualTo(1);
    }

    @Test
    void concurrentDeliveriesOfTheSameEventProduceOneEffect() throws Exception {
        AtomicInteger effects = new AtomicInteger();
        ExecutorService pool = Executors.newFixedThreadPool(8);
        try {
            List<Future<IdempotentConsumer.Outcome>> results = new ArrayList<>();
            for (int i = 0; i < 16; i++) {
                results.add(pool.submit(() -> consumer.process("inventory", event, e -> effects.incrementAndGet())));
            }
            int processed = 0;
            for (var f : results) {
                if (f.get() == IdempotentConsumer.Outcome.PROCESSED) {
                    processed++;
                }
            }
            assertThat(processed).isEqualTo(1);
            assertThat(effects).hasValue(1);
        } finally {
            pool.shutdownNow();
        }
    }
}
