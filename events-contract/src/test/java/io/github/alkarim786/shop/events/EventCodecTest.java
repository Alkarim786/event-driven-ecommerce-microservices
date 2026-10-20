package io.github.alkarim786.shop.events;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class EventCodecTest {

    private final EventCodec codec = new EventCodec();

    @Test
    void envelopeRoundTripsThroughJson() {
        var payload = new Payloads.OrderCreated("o-1", "c-1",
                List.of(new Payloads.LineItem("SKU-1", 2)), 1998, "tok_ok");
        EventEnvelope env = codec.envelope(EventTypes.ORDER_CREATED, "o-1", payload);

        EventEnvelope parsed = codec.parse(codec.toJson(env));

        assertThat(parsed.eventId()).isEqualTo(env.eventId());
        assertThat(parsed.type()).isEqualTo(EventTypes.ORDER_CREATED);
        assertThat(parsed.occurredAt()).isEqualTo(env.occurredAt());
        assertThat(codec.payload(parsed, Payloads.OrderCreated.class)).isEqualTo(payload);
    }

    @Test
    void unknownFieldsAreIgnoredForForwardCompatibility() {
        String json = """
                {"eventId":"8d3c6a3e-2b0f-4c47-9d0e-1f2a3b4c5d6e","type":"StockRejected",
                 "aggregateId":"o-9","occurredAt":"2026-10-19T10:00:00Z","version":2,
                 "payload":{"orderId":"o-9","reason":"out of stock","newField":true},
                 "traceId":"abc"}""";
        EventEnvelope env = codec.parse(json);
        assertThat(codec.payload(env, Payloads.StockRejected.class).reason()).isEqualTo("out of stock");
    }

    @Test
    void poisonMessagesAreRejectedWithATypedException() {
        assertThatThrownBy(() -> codec.parse("not json"))
                .isInstanceOf(EventCodec.MalformedEventException.class);
        assertThatThrownBy(() -> codec.parse("{\"type\":\"OrderCreated\"}"))
                .isInstanceOf(EventCodec.MalformedEventException.class);
    }
}
