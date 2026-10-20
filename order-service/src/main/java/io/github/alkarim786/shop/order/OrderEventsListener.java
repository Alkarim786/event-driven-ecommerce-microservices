package io.github.alkarim786.shop.order;

import io.github.alkarim786.shop.events.EventCodec;
import io.github.alkarim786.shop.events.Topics;
import io.github.alkarim786.shop.messaging.IdempotentConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/** Kafka entry point: parse, then process exactly once per event id. */
@Component
class OrderEventsListener {

    private final EventCodec codec;
    private final IdempotentConsumer idempotent;
    private final OrderSagaHandler handler;

    OrderEventsListener(EventCodec codec, IdempotentConsumer idempotent, OrderSagaHandler handler) {
        this.codec = codec;
        this.idempotent = idempotent;
        this.handler = handler;
    }

    @KafkaListener(topics = {Topics.INVENTORY, Topics.PAYMENTS}, groupId = OrderSagaHandler.CONSUMER_GROUP)
    void onEvent(ConsumerRecord<String, String> record) {
        idempotent.process(OrderSagaHandler.CONSUMER_GROUP, codec.parse(record.value()), handler::handle);
    }
}
