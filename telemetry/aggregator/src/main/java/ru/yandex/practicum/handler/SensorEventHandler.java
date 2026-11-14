package ru.yandex.practicum.handler;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecordBase;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;
import ru.yandex.practicum.service.SnapshotService;

import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Slf4j
@Component
@RequiredArgsConstructor
public class SensorEventHandler {

    private final SnapshotService snapshotService;
    private final KafkaTemplate<String, SpecificRecordBase> kafkaTemplate;

    @Value("${kafka.topics.snapshots:telemetry.snapshots.v1}")
    private String snapshotsTopic;

    @KafkaListener(
            topics = "${kafka.topics.sensors:telemetry.sensors.v1}",
            groupId = "${kafka.consumer.group-id:aggregator}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void handleSensorEvent(SensorEventAvro event, Acknowledgment ack) {
        log.debug("Received sensor event: hubId={}, sensorId={}",
                event.getHubId(), event.getId());

        try {
            Optional<SensorsSnapshotAvro> updatedSnapshot = snapshotService.updateState(event);

            if (updatedSnapshot.isPresent()) {
                SensorsSnapshotAvro snapshot = updatedSnapshot.get();
                kafkaTemplate.send(snapshotsTopic, snapshot.getHubId(), snapshot)
                        .get(5, TimeUnit.SECONDS);

                log.info("Snapshot sent to Kafka: hubId={}, sensors_count={}",
                        snapshot.getHubId(), snapshot.getSensorsState().size());
            } else {
                log.debug("Snapshot not updated: hubId={}, sensorId={}",
                        event.getHubId(), event.getId());
            }

            ack.acknowledge();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Interrupted while sending snapshot for hubId={}", event.getHubId(), e);
            throw new RuntimeException("Interrupted while processing sensor event", e);
        } catch (ExecutionException | TimeoutException e) {
            log.error("Failed to send snapshot to Kafka for hubId={}", event.getHubId(), e);
            throw new RuntimeException("Failed to send snapshot to Kafka", e);
        }
    }
}