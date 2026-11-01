package ru.practicum.controller;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import ru.practicum.model.hubs.HubEvent;
import ru.practicum.model.sensors.SensorEvent;
import ru.practicum.service.EventService;

@RestController
@RequestMapping("/events")
@RequiredArgsConstructor
public class EventController {
    private final EventService eventService;

    @PostMapping("/sensors")
    public ResponseEntity<Void> collectSensorEvents(@Valid @RequestBody SensorEvent sensorEvent) {
        eventService.publishSensorEvent(sensorEvent);
        return ResponseEntity.ok().build();
    }

    @PostMapping("/hubs")
    public ResponseEntity<Void> collectHubEvents(@Valid @RequestBody HubEvent hubEvent) {
        eventService.publishHubEvent(hubEvent);
        return ResponseEntity.ok().build();
    }
}
