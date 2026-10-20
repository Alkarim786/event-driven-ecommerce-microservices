package io.github.alkarim786.shop.messaging;

import io.github.alkarim786.shop.events.EventCodec;
import io.github.alkarim786.shop.events.EventEnvelope;
import io.github.alkarim786.shop.messaging.testing.RecordingPublisher;
import io.github.alkarim786.shop.messaging.testing.TestDatabase;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class OutboxRelayTest {

    private final TestDatabase db = TestDatabase.h2();
    private final EventCodec codec = new EventCodec();
    private final OutboxWriter writer = new OutboxWriter(db.jdbc, codec);
    private final RecordingPublisher publisher = new RecordingPublisher();
    private final OutboxRelay relay = new OutboxRelay(db.jdbc, db.tx, publisher,
            OutboxProperties.defaults(), Clock.systemUTC());

    @AfterEach
    void close() {
        relay.stop();
        db.close();
    }

    private EventEnvelope append(String orderId, String type) {
        return db.tx.execute(s -> writer.append("orders.events", type, orderId, Map.of("orderId", orderId)));
    }

    @Test
    void appendOutsideATransactionFailsFast() {
        assertThatThrownBy(() -> writer.append("orders.events", "OrderCreated", "o-1", Map.of()))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void eventRolledBackWithTheBusinessTransactionIsNeverPublished() {
        try {
            db.tx.executeWithoutResult(s -> {
                writer.append("orders.events", "OrderCreated", "o-1", Map.of());
                throw new IllegalStateException("business rule failed");
            });
        } catch (IllegalStateException expected) {
            // the outbox row must vanish together with the business change
        }
        assertThat(relay.relayOnce()).isZero();
        assertThat(publisher.messages()).isEmpty();
    }

    @Test
    void publishesInCommitOrderKeyedByAggregateAndMarksRowsPublished() {
        append("o-1", "OrderCreated");
        append("o-2", "OrderCreated");
        append("o-1", "OrderConfirmed");

        assertThat(relay.relayOnce()).isEqualTo(3);

        List<RecordingPublisher.Message> sent = publisher.messages();
        assertThat(sent).extracting(RecordingPublisher.Message::key).containsExactly("o-1", "o-2", "o-1");
        assertThat(sent).extracting(m -> codec.parse(m.value()).type())
                .containsExactly("OrderCreated", "OrderCreated", "OrderConfirmed");
        assertThat(relay.pendingCount()).isZero();
        assertThat(relay.relayOnce()).isZero(); // nothing is sent twice in the happy path
    }

    @Test
    void brokerFailureRollsBackSoTheBatchIsRetriedLater() {
        append("o-1", "OrderCreated");
        append("o-2", "OrderCreated");
        publisher.failNext(1);

        assertThatThrownBy(relay::relayOnce).isInstanceOf(OutboxRelay.OutboxPublishException.class);
        assertThat(relay.pendingCount()).isEqualTo(2);

        assertThat(relay.relayOnce()).isEqualTo(2);
        assertThat(relay.pendingCount()).isZero();
        // o-2 was acked during the failed pass, so it is delivered again: at-least-once.
        assertThat(publisher.messages()).extracting(RecordingPublisher.Message::key)
                .containsExactly("o-2", "o-1", "o-2");
    }

    @Test
    void chaosModePublishesDuplicates() {
        OutboxRelay chaos = new OutboxRelay(db.jdbc, db.tx, publisher,
                OutboxProperties.defaults().withChaosDuplicateRate(1.0), Clock.systemUTC());
        append("o-1", "OrderCreated");
        chaos.relayOnce();
        assertThat(publisher.messages()).hasSize(2);
        assertThat(chaos.chaosDuplicates()).isEqualTo(1);
    }

    @Test
    void purgeDeletesOnlyPublishedRows() {
        append("o-1", "OrderCreated");
        relay.relayOnce();
        append("o-2", "OrderCreated");

        assertThat(relay.purgePublished(Duration.ofMillis(-1000))).isEqualTo(1);
        assertThat(db.count("SELECT COUNT(*) FROM outbox")).isEqualTo(1);
    }

    @Test
    void backgroundLoopIsWokenByCommitInsteadOfWaitingForThePollInterval() throws Exception {
        OutboxProperties slowPoll = new OutboxProperties(true, 200, Duration.ofSeconds(30),
                Duration.ofSeconds(1), Duration.ofSeconds(5), Duration.ofHours(1), "FOR UPDATE SKIP LOCKED", 0);
        OutboxRelay background = new OutboxRelay(db.jdbc, db.tx, publisher, slowPoll, Clock.systemUTC());
        CountDownLatch published = new CountDownLatch(1);
        writer.onCommit(() -> {
            background.wakeUp();
            published.countDown();
        });
        background.start();
        try {
            Thread.sleep(100); // let the loop do its first (empty) pass and go to sleep
            append("o-1", "OrderCreated");
            assertThat(published.await(2, TimeUnit.SECONDS)).isTrue();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
            while (publisher.messages().isEmpty() && System.nanoTime() < deadline) {
                Thread.sleep(10);
            }
            assertThat(publisher.messages()).hasSize(1); // well under the 30 s poll interval
        } finally {
            background.stop();
        }
    }
}
