package io.github.alkarim786.shop.events;

import com.fasterxml.jackson.databind.JsonNode;

import java.time.Instant;
import java.util.UUID;

/**
 * Wire format for every event on every topic.
 *
 * @param eventId     unique per event; consumers de-duplicate on it
 * @param type        one of {@link EventTypes}
 * @param aggregateId the order id; also the Kafka message key, so all events of one order stay ordered
 * @param occurredAt  when the producing transaction wrote the event
 * @param version     schema version of the payload, for forward-compatible evolution
 * @param payload     type-specific body (see {@link Payloads})
 */
public record EventEnvelope(
        UUID eventId,
        String type,
        String aggregateId,
        Instant occurredAt,
        int version,
        JsonNode payload) {

    public static final int CURRENT_VERSION = 1;
}
