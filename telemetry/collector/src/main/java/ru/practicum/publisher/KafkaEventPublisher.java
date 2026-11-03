package ru.practicum.publisher;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.stereotype.Component;
import ru.practicum.exception.KafkaPublishException;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.practicum.kafka.KafkaTopicsProperties;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaEventPublisher implements EventPublisher {

    private final Producer<String, SpecificRecordBase> kafkaProducer;
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
        ProducerRecord<String, SpecificRecordBase> record = new ProducerRecord<>(
                topic, null, timestamp, key, value);

        CompletableFuture<RecordMetadata> future = new CompletableFuture<>();

        kafkaProducer.send(record, (metadata, ex) -> {
            if (ex != null) {
                future.completeExceptionally(ex);
            } else {
                future.complete(metadata);
            }
        });

        try {
            RecordMetadata metadata = future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Failed to send message to topic '{}': {}", topic, e.getMessage(), e);
            throw new KafkaPublishException("Отправка в Kafka прервана");
        } catch (ExecutionException e) {
            log.error("Failed to send message to topic '{}': {}", topic, e.getCause().getMessage(), e.getCause());
            throw new KafkaPublishException("Ошибка отправки в Kafka");
        }
    }
}