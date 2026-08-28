package ru.yandex.practicum.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.grpc.HubRouterClient;
import ru.yandex.practicum.model.enm.ActionType;
import ru.yandex.practicum.model.enm.ConditionType;
import ru.yandex.practicum.model.entity.Action;
import ru.yandex.practicum.model.entity.Condition;
import ru.yandex.practicum.model.entity.Scenario;
import ru.yandex.practicum.model.entity.Sensor;
import ru.yandex.practicum.model.entity.scenario.ScenarioAction;
import ru.yandex.practicum.model.entity.scenario.ScenarioCondition;
import ru.yandex.practicum.repository.ScenarioRepository;
import ru.yandex.practicum.grpc.telemetry.event.ActionTypeProto;
import ru.yandex.practicum.grpc.telemetry.event.DeviceActionProto;
import ru.yandex.practicum.kafka.telemetry.event.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Service
@RequiredArgsConstructor
public class SnapshotServiceImpl implements SnapshotService {

    private final ScenarioRepository scenarioRepository;
    private final HubRouterClient hubRouterClient;

    @Override
    @Transactional(readOnly = true)
    public void analyzeSnapshot(SensorsSnapshotAvro snapshot) {
        String hubId = snapshot.getHubId().toString();
        log.debug("Analyzing snapshot for hub: {}", hubId);

        List<Scenario> scenarios = scenarioRepository.findByHubId(hubId);
        if (scenarios.isEmpty()) {
            log.debug("No scenarios found for hub {}", hubId);
            return;
        }

        Map<String, SensorStateAvro> sensorsState = new HashMap<>();
        snapshot.getSensorsState().forEach((key, value) -> sensorsState.put(key.toString(), value));

        for (Scenario scenario : scenarios) {
            if (checkScenario(scenario, sensorsState)) {
                log.info("Scenario '{}' matched for hub {}", scenario.getName(), hubId);
                executeActions(scenario, hubId, sensorsState);
            }
        }
    }

    private boolean checkScenario(Scenario scenario, Map<String, SensorStateAvro> sensorsState) {
        for (ScenarioCondition sc : scenario.getConditions()) {
            Sensor sensor = sc.getSensor();
            Condition condition = sc.getCondition();

            SensorStateAvro state = sensorsState.get(sensor.getId());
            if (state == null) {
                log.debug("Sensor {} not found in snapshot", sensor.getId());
                return false;
            }

            if (!checkCondition(condition, state)) {
                return false;
            }
        }
        return true;
    }

    private boolean checkCondition(Condition condition, SensorStateAvro state) {
        Object data = state.getData();
        Integer actualValue = extractValue(data, condition.getType());

        if (actualValue == null) {
            log.debug("Cannot extract value for condition type {}", condition.getType());
            return false;
        }

        Integer expectedValue = condition.getValue();
        if (expectedValue == null) {
            log.debug("Condition value is null");
            return false;
        }

        return switch (condition.getOperation()) {
            case EQUALS -> actualValue.equals(expectedValue);
            case GREATER_THAN -> actualValue > expectedValue;
            case LOWER_THAN -> actualValue < expectedValue;
        };
    }

    private Integer extractValue(Object sensorData, ConditionType type) {
        return switch (type) {
            case TEMPERATURE -> {
                if (sensorData instanceof TemperatureSensorAvro t) {
                    yield t.getTemperatureC();
                }
                yield null;
            }
            case HUMIDITY, CO2LEVEL -> {
                if (sensorData instanceof ClimateSensorAvro c) {
                    yield switch (type) {
                        case HUMIDITY -> c.getHumidity();
                        case CO2LEVEL -> c.getCo2Level();
                        default -> null;
                    };
                }
                yield null;
            }
            case LUMINOSITY -> {
                if (sensorData instanceof LightSensorAvro l) {
                    yield l.getLuminosity();
                }
                yield null;
            }
            case MOTION -> {
                if (sensorData instanceof MotionSensorAvro m) {
                    yield m.getMotion() ? 1 : 0;
                }
                yield null;
            }
            case SWITCH -> {
                if (sensorData instanceof SwitchSensorAvro s) {
                    yield s.getState() ? 1 : 0;
                }
                yield null;
            }
        };
    }

    private void executeActions(Scenario scenario, String hubId, Map<String, SensorStateAvro> sensorsState) {
        for (ScenarioAction sa : scenario.getActions()) {
            Sensor sensor = sa.getSensor();
            Action action = sa.getAction();

            DeviceActionProto protoAction = DeviceActionProto.newBuilder()
                    .setSensorId(sensor.getId())
                    .setType(convertActionType(action.getType()))
                    .setValue(action.getValue())
                    .build();

            try {
                hubRouterClient.sendDeviceAction(hubId, scenario.getName(), protoAction);
            } catch (Exception e) {
                log.error("Failed to execute action for scenario '{}': {}", scenario.getName(), e.getMessage());
            }
        }
    }

    private ActionTypeProto convertActionType(ActionType type) {
        return switch (type) {
            case ACTIVATE -> ActionTypeProto.ACTIVATE;
            case DEACTIVATE -> ActionTypeProto.DEACTIVATE;
            case INVERSE -> ActionTypeProto.INVERSE;
            case SET_VALUE -> ActionTypeProto.SET_VALUE;
        };
    }
}