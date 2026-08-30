package ru.yandex.practicum.model.entity;

import jakarta.persistence.*;
import lombok.*;
import ru.yandex.practicum.model.entity.scenario.ScenarioAction;
import ru.yandex.practicum.model.entity.scenario.ScenarioCondition;

import java.util.HashSet;
import java.util.Set;

@Entity
@Table(name = "scenarios")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Scenario {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "hub_id")
    private String hubId;

    @Column(name = "name")
    private String name;

    @OneToMany(mappedBy = "scenario", cascade = CascadeType.ALL, orphanRemoval = true)
    @Builder.Default
    private Set<ScenarioCondition> conditions = new HashSet<>();

    @OneToMany(mappedBy = "scenario", cascade = CascadeType.ALL, orphanRemoval = true)
    @Builder.Default
    private Set<ScenarioAction> actions = new HashSet<>();

}
