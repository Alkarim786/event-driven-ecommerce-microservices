package io.github.alkarim786.shop.messaging.testing;

import io.github.alkarim786.shop.messaging.EventPublisher;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

/** In-memory {@link EventPublisher} that records messages and can be told to fail. */
public class RecordingPublisher implements EventPublisher {

    public record Message(String topic, String key, String value) {}

    private final List<Message> messages = new ArrayList<>();
    private final AtomicInteger failNext = new AtomicInteger();

    /** Makes the next {@code n} publish calls fail (simulates a broker outage). */
    public void failNext(int n) {
        failNext.set(n);
    }

    @Override
    public synchronized CompletableFuture<Void> publish(String topic, String key, String value) {
        if (failNext.getAndUpdate(n -> Math.max(0, n - 1)) > 0) {
            return CompletableFuture.failedFuture(new IllegalStateException("broker unavailable"));
        }
        messages.add(new Message(topic, key, value));
        return CompletableFuture.completedFuture(null);
    }

    public synchronized List<Message> messages() {
        return List.copyOf(messages);
    }

    public synchronized List<Message> drain() {
        List<Message> out = List.copyOf(messages);
        messages.clear();
        return out;
    }
}
