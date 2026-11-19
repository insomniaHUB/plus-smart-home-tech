package ru.yandex.practicum.kafka;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.service.HubEventService;
import ru.yandex.practicum.service.SnapshotService;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Component
@Slf4j
@RequiredArgsConstructor
public class ConsumersRunner {

    private final HubEventService hubEventService;
    private final SnapshotService snapshotService;

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${kafka.topics.snapshots}")
    private String snapshotsTopic;

    @Value("${kafka.topics.hubs}")
    private String hubsTopic;

    @Value("${kafka.consumer.snapshot.group-id}")
    private String snapshotGroupId;

    @Value("${kafka.consumer.hub.group-id}")
    private String hubGroupId;

    private SnapshotConsumer snapshotConsumer;
    private HubEventConsumer hubEventConsumer;
    private ExecutorService executorService;

    @PostConstruct
    public void start() {
        snapshotConsumer = new SnapshotConsumer(bootstrapServers, snapshotsTopic, snapshotGroupId, snapshotService);
        hubEventConsumer = new HubEventConsumer(bootstrapServers, hubsTopic, hubGroupId, hubEventService);

        executorService = Executors.newFixedThreadPool(2);
        executorService.submit(snapshotConsumer);
        executorService.submit(hubEventConsumer);

        log.info("Kafka consumers started successfully");
    }

    @PreDestroy
    public void stop() {
        if (snapshotConsumer != null) {
            snapshotConsumer.stop();
        }
        if (hubEventConsumer != null) {
            hubEventConsumer.stop();
        }

        if (executorService != null) {
            executorService.shutdown();
            try {
                if (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
                    executorService.shutdownNow();
                }
            } catch (InterruptedException e) {
                executorService.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        log.info("Kafka consumers stopped");
    }
}
