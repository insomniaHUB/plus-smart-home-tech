package ru.practicum.handler.sensor;

import org.springframework.stereotype.Component;
import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;
import ru.yandex.practicum.grpc.telemetry.event.TemperatureSensorProto;
import ru.yandex.practicum.kafka.telemetry.event.TemperatureSensorAvro;
import ru.practicum.publisher.EventPublisher;

@Component
public class TemperatureSensorEventHandler extends AbstractSensorEventHandler {

    public TemperatureSensorEventHandler(EventPublisher eventPublisher) {
        super(eventPublisher);
    }

    @Override
    public SensorEventProto.PayloadCase getMessageType() {
        return SensorEventProto.PayloadCase.TEMPERATURE_SENSOR;
    }

    @Override
    public void handle(SensorEventProto event) {
        TemperatureSensorProto proto = event.getTemperatureSensor();

        TemperatureSensorAvro avro = TemperatureSensorAvro.newBuilder()
                .setTemperatureC(proto.getTemperatureC())
                .setTemperatureF(proto.getTemperatureF())
                .build();

        publish(buildSensorEvent(event, avro));
    }
}