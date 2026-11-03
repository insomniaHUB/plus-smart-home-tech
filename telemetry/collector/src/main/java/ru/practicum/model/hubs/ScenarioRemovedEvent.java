package ru.practicum.model.hubs;

import jakarta.validation.constraints.NotBlank;
import lombok.Data;

@Data
public class ScenarioRemovedEvent extends HubEvent {
    @NotBlank
    private String name;

    @Override
    public HubEventType getType() {
        return HubEventType.SCENARIO_REMOVED;
    }
}
