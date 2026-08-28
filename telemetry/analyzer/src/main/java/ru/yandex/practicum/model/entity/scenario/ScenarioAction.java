package ru.yandex.practicum.model.entity.scenario;

import jakarta.persistence.*;
import lombok.*;
import ru.yandex.practicum.model.entity.Action;
import ru.yandex.practicum.model.entity.Scenario;
import ru.yandex.practicum.model.entity.Sensor;

@Entity
@Table(name = "scenario_actions")
@IdClass(ScenarioActionId.class)
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ScenarioAction {

    @Id
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "scenario_id")
    private Scenario scenario;

    @Id
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "sensor_id")
    private Sensor sensor;

    @Id
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "action_id")
    private Action action;
}
