package io.github.alkarim786.shop.inventory;

import io.github.alkarim786.shop.events.EventCodec;
import io.github.alkarim786.shop.events.Topics;
import io.github.alkarim786.shop.messaging.IdempotentConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
class InventoryEventsListener {

    private final EventCodec codec;
    private final IdempotentConsumer idempotent;
    private final InventoryHandler handler;

    InventoryEventsListener(EventCodec codec, IdempotentConsumer idempotent, InventoryHandler handler) {
        this.codec = codec;
        this.idempotent = idempotent;
        this.handler = handler;
    }

    @KafkaListener(topics = {Topics.ORDERS, Topics.PAYMENTS}, groupId = InventoryHandler.CONSUMER_GROUP)
    void onEvent(ConsumerRecord<String, String> record) {
        idempotent.process(InventoryHandler.CONSUMER_GROUP, codec.parse(record.value()), handler::handle);
    }
}
