package ru.practicum.mapper;

import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.Named;
import ru.yandex.practicum.kafka.telemetry.event.DeviceActionAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioConditionAvro;
import ru.practicum.model.hubs.DeviceAction;
import ru.practicum.model.hubs.ScenarioCondition;

@Mapper(componentModel = "spring")
public interface ScenarioComponentMapper {

    @Mapping(target = "value", source = "value", qualifiedByName = "mapConditionValue")
    ScenarioConditionAvro toConditionAvro(ScenarioCondition condition);

    @Mapping(target = "value", source = "value")
    DeviceActionAvro toActionAvro(DeviceAction action);

    @Named("mapConditionValue")
    default Object mapConditionValue(Object value) {
        return value;
    }
}