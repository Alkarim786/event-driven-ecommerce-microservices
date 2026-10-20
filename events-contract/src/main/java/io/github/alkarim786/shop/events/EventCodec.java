package io.github.alkarim786.shop.events;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.time.Instant;
import java.util.UUID;

/** JSON (de)serialisation of envelopes and payloads. Thread-safe. */
public final class EventCodec {

    private final ObjectMapper mapper;

    public EventCodec() {
        this.mapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
                // Tolerant reader: new optional fields from a newer producer are ignored.
                .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
    }

    public ObjectMapper mapper() {
        return mapper;
    }

    /** Builds a new envelope with a fresh event id. */
    public EventEnvelope envelope(String type, String aggregateId, Object payload) {
        return new EventEnvelope(UUID.randomUUID(), type, aggregateId, Instant.now(),
                EventEnvelope.CURRENT_VERSION, mapper.valueToTree(payload));
    }

    public String toJson(Object value) {
        try {
            return mapper.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("cannot serialise " + value.getClass().getSimpleName(), e);
        }
    }

    /** Parses an envelope. Throws {@link MalformedEventException} for poison messages. */
    public EventEnvelope parse(String json) {
        try {
            EventEnvelope env = mapper.readValue(json, EventEnvelope.class);
            if (env.eventId() == null || env.type() == null || env.aggregateId() == null) {
                throw new MalformedEventException("envelope missing eventId/type/aggregateId");
            }
            return env;
        } catch (JsonProcessingException e) {
            throw new MalformedEventException("unparseable event: " + e.getOriginalMessage(), e);
        }
    }

    public <T> T payload(EventEnvelope env, Class<T> type) {
        try {
            return mapper.treeToValue(env.payload(), type);
        } catch (JsonProcessingException e) {
            throw new MalformedEventException("bad " + env.type() + " payload: " + e.getOriginalMessage(), e);
        }
    }

    public static final class MalformedEventException extends RuntimeException {
        public MalformedEventException(String message) {
            super(message);
        }

        public MalformedEventException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
