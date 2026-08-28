package ru.yandex.practicum.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.model.enm.ActionType;
import ru.yandex.practicum.model.enm.ConditionOperation;
import ru.yandex.practicum.model.enm.ConditionType;
import ru.yandex.practicum.model.entity.Action;
import ru.yandex.practicum.model.entity.Condition;
import ru.yandex.practicum.model.entity.Scenario;
import ru.yandex.practicum.model.entity.Sensor;
import ru.yandex.practicum.model.entity.scenario.ScenarioAction;
import ru.yandex.practicum.model.entity.scenario.ScenarioCondition;
import ru.yandex.practicum.repository.*;
import ru.yandex.practicum.kafka.telemetry.event.*;

import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class HubEventServiceImpl implements HubEventService {

    private final SensorRepository sensorRepository;
    private final ScenarioRepository scenarioRepository;
    private final ConditionRepository conditionRepository;
    private final ActionRepository actionRepository;
    private final ScenarioConditionRepository scenarioConditionRepository;
    private final ScenarioActionRepository scenarioActionRepository;

    @Override
    @Transactional
    public void handle(HubEventAvro event) {
        String hubId = event.getHubId().toString();
        Object payload = event.getPayload();

        if (payload instanceof DeviceAddedEventAvro e) {
            addDevice(hubId, e);
        } else if (payload instanceof DeviceRemovedEventAvro e) {
            removeDevice(hubId, e);
        } else if (payload instanceof ScenarioAddedEventAvro e) {
            addScenario(hubId, e);
        } else if (payload instanceof ScenarioRemovedEventAvro e) {
            removeScenario(hubId, e);
        } else {
            log.warn("Unknown hub event payload type: {}", payload.getClass().getSimpleName());
        }
    }

    private void addDevice(String hubId, DeviceAddedEventAvro e) {
        String sensorId = e.getId().toString();
        Sensor sensor = sensorRepository.findById(Long.valueOf(sensorId))
                .map(existing -> {
                    existing.setHubId(hubId);
                    return existing;
                })
                .orElseGet(() -> Sensor.builder().id(sensorId).hubId(hubId).build());
        sensorRepository.save(sensor);
        log.info("Device added: {} (hub: {})", sensorId, hubId);
    }

    private void removeDevice(String hubId, DeviceRemovedEventAvro e) {
        String sensorId = e.getId().toString();
        sensorRepository.findByIdAndHubId(sensorId, hubId).ifPresentOrElse(sensor -> {
            scenarioConditionRepository.deleteAllBySensorId(sensorId);
            scenarioActionRepository.deleteAllBySensorId(sensorId);
            sensorRepository.delete(sensor);
            log.info("Device removed: {} (hub: {})", sensorId, hubId);
        }, () -> log.debug("Device {} already removed — skipping", sensorId));
    }

    private void addScenario(String hubId, ScenarioAddedEventAvro e) {
        String name = e.getName().toString();

        scenarioRepository.findByHubIdAndName(hubId, name)
                .ifPresent(this::deleteScenarioFully);

        Scenario scenario = Scenario.builder().hubId(hubId).name(name).build();

        for (ScenarioConditionAvro c : e.getConditions()) {
            String sensorId = c.getSensorId().toString();
            sensorRepository.findByIdAndHubId(sensorId, hubId).ifPresentOrElse(sensor -> {
                Condition condition = conditionRepository.save(Condition.builder()
                        .type(ConditionType.valueOf(c.getType().name()))
                        .operation(ConditionOperation.valueOf(c.getOperation().name()))
                        .value(toInt(c.getValue()))
                        .build());
                scenario.getConditions().add(ScenarioCondition.builder()
                        .scenario(scenario).sensor(sensor).condition(condition).build());
            }, () -> log.warn("Sensor {} not found in hub {} — condition skipped", sensorId, hubId));
        }

        for (DeviceActionAvro a : e.getActions()) {
            String sensorId = a.getSensorId().toString();
            sensorRepository.findByIdAndHubId(sensorId, hubId).ifPresentOrElse(sensor -> {
                Action action = actionRepository.save(Action.builder()
                        .type(ActionType.valueOf(a.getType().name()))
                        .value(a.getValue())
                        .build());
                scenario.getActions().add(ScenarioAction.builder()
                        .scenario(scenario).sensor(sensor).action(action).build());
            }, () -> log.warn("Sensor {} not found in hub {} — action skipped", sensorId, hubId));
        }

        scenarioRepository.save(scenario);
        log.info(" Scenario saved: '{}' (conditions: {}, actions: {})",
                name, scenario.getConditions().size(), scenario.getActions().size());
    }

    private void removeScenario(String hubId, ScenarioRemovedEventAvro e) {
        String name = e.getName().toString();
        scenarioRepository.findByHubIdAndName(hubId, name).ifPresentOrElse(scenario -> {
            deleteScenarioFully(scenario);
            log.info("Scenario removed: '{}' (hub: {})", name, hubId);
        }, () -> log.debug("Scenario '{}' already removed — skipping", name));
    }

    private void deleteScenarioFully(Scenario scenario) {
        List<Condition> conditions = scenario.getConditions().stream()
                .map(ScenarioCondition::getCondition).toList();
        List<Action> actions = scenario.getActions().stream()
                .map(ScenarioAction::getAction).toList();

        scenarioRepository.delete(scenario);
        scenarioRepository.flush();

        conditionRepository.deleteAll(conditions);
        actionRepository.deleteAll(actions);
    }

    private Integer toInt(Object value) {
        if (value instanceof Integer i) return i;
        if (value instanceof Boolean b) return b ? 1 : 0;
        return null;
    }
}