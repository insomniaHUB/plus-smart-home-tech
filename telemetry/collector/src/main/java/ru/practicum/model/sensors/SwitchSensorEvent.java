package ru.practicum.model.sensors;

import jakarta.validation.constraints.NotNull;
import lombok.Data;

@Data
public class SwitchSensorEvent extends SensorEvent {
    @NotNull
    private Boolean state;

    @Override
    public SensorEventType getType() {
        return SensorEventType.SWITCH_SENSOR_EVENT;
    }
}
