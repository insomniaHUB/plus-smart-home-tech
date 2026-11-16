package ru.yandex.practicum.handler;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;
import ru.yandex.practicum.service.SnapshotService;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Slf4j
@Component
@RequiredArgsConstructor
public class SensorEventHandler {

    private final SnapshotService snapshotService;
    private final KafkaConsumer<String, SensorEventAvro> consumer;
    private final KafkaProducer<String, SpecificRecordBase> producer;

    @Value("${kafka.topics.sensors:telemetry.sensors.v1}")
    private String sensorsTopic;

    @Value("${kafka.topics.snapshots:telemetry.snapshots.v1}")
    private String snapshotsTopic;

    private final Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();

    public void start() {
        Runtime.getRuntime().addShutdownHook(new Thread(consumer::wakeup));

        try {
            consumer.subscribe(List.of(sensorsTopic));

            while (true) {
                ConsumerRecords<String, SensorEventAvro> records = consumer.poll(Duration.ofMillis(100));

                int count = 0;
                for (ConsumerRecord<String, SensorEventAvro> record : records) {
                    handleRecord(record);
                    manageOffsets(record, count);
                    count++;
                }
                consumer.commitAsync();
            }
        } catch (WakeupException ignored) {
            log.info("Consumer wakeup triggered");
        } catch (Exception e) {
            log.error("Error while processing sensor events", e);
        } finally {
            try {
                producer.flush();
                consumer.commitSync(currentOffsets);
            } finally {
                consumer.close();
                producer.close();
            }
        }
    }

    private void handleRecord(ConsumerRecord<String, SensorEventAvro> record) {
        SensorEventAvro event = record.value();
        log.debug("Received sensor event: hubId={}, sensorId={}", event.getHubId(), event.getId());

        Optional<SensorsSnapshotAvro> updatedSnapshot = snapshotService.updateState(event);
        if (updatedSnapshot.isPresent()) {
            SensorsSnapshotAvro snapshot = updatedSnapshot.get();
            ProducerRecord<String, SpecificRecordBase> producerRecord = new ProducerRecord<>(
                    snapshotsTopic,
                    null,
                    snapshot.getTimestamp().toEpochMilli(),
                    snapshot.getHubId(),
                    snapshot
            );

            try {
                producer.send(producerRecord).get(5, TimeUnit.SECONDS);
                log.info("Snapshot sent to Kafka: hubId={}, sensors_count={}",
                        snapshot.getHubId(), snapshot.getSensorsState().size());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.error("Interrupted while sending snapshot for hubId={}", event.getHubId(), e);
                throw new RuntimeException("Interrupted while processing sensor event", e);
            } catch (ExecutionException | TimeoutException e) {
                log.error("Failed to send snapshot to Kafka for hubId={}", event.getHubId(), e);
                throw new RuntimeException("Failed to send snapshot to Kafka", e);
            }
        } else {
            log.debug("Snapshot not updated: hubId={}, sensorId={}", event.getHubId(), event.getId());
        }
    }

    private void manageOffsets(ConsumerRecord<String, SensorEventAvro> record, int count) {
        currentOffsets.put(
                new TopicPartition(record.topic(), record.partition()),
                new OffsetAndMetadata(record.offset() + 1)
        );
        if (count % 10 == 0) {
            consumer.commitAsync(currentOffsets, (offsets, exception) -> {
                if (exception != null) {
                    log.error("Error committing offsets: {}", offsets, exception);
                }
            });
        }
    }
}
