package ru.practicum.service;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import ru.practicum.mapper.HubEventMapper;
import ru.practicum.mapper.SensorEventMapper;
import ru.practicum.model.hubs.HubEvent;
import ru.practicum.model.sensors.SensorEvent;
import ru.practicum.publisher.EventPublisher;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;

@Service
@RequiredArgsConstructor
public class EventService {
    private final SensorEventMapper sensorEventMapper;
    private final HubEventMapper hubEventMapper;
    private final EventPublisher eventPublisher;


    public void publishSensorEvent(SensorEvent event) {
        SensorEventAvro sensorEventAvro = sensorEventMapper.toAvro(event);
        eventPublisher.publishSensorEvent(sensorEventAvro);
    }

    public void publishHubEvent(HubEvent event) {
        HubEventAvro hubEventAvro = hubEventMapper.toAvro(event);
        eventPublisher.publishHubEvent(hubEventAvro);
    }
}
