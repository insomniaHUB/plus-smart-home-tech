package ru.practicum.publisher;

import lombok.RequiredArgsConstructor;
import org.apache.avro.specific.SpecificRecordBase;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.practicum.kafka.KafkaTopicsProperties;

import java.util.concurrent.CompletableFuture;

@Component
@RequiredArgsConstructor
public class KafkaEventPublisher implements EventPublisher {

    private final KafkaTemplate<String, SpecificRecordBase> kafkaTemplate;
    private final KafkaTopicsProperties topics;

    @Override
    public void publishSensorEvent(SensorEventAvro event) {
        send(topics.sensors(), event.getHubId(), event.getTimestamp().toEpochMilli(), event);
    }

    @Override
    public void publishHubEvent(HubEventAvro event) {
        send(topics.hubs(), event.getHubId(), event.getTimestamp().toEpochMilli(), event);
    }

    private void send(String topic, String key, long timestamp, SpecificRecordBase value) {
        CompletableFuture<SendResult<String, SpecificRecordBase>> future =
                kafkaTemplate.send(topic, null, timestamp, key, value);
    }
}