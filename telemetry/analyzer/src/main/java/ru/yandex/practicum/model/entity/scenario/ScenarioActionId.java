package ru.yandex.practicum.model.entity.scenario;

import lombok.*;

import java.io.Serializable;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode
public class ScenarioActionId implements Serializable {
    private Long scenario;
    private String sensor;
    private Long action;
}
