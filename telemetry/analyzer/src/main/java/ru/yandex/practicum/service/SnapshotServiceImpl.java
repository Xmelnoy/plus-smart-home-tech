package ru.yandex.practicum.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.grpc.HubRouterClient;
import ru.yandex.practicum.grpc.telemetry.event.ActionTypeProto;
import ru.yandex.practicum.grpc.telemetry.event.DeviceActionProto;
import ru.yandex.practicum.kafka.telemetry.event.*;
import ru.yandex.practicum.model.enm.ActionType;
import ru.yandex.practicum.model.enm.ConditionType;
import ru.yandex.practicum.model.entity.*;
import ru.yandex.practicum.model.entity.scenario.ScenarioAction;
import ru.yandex.practicum.model.entity.scenario.ScenarioCondition;
import ru.yandex.practicum.repository.ScenarioRepository;

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
        log.info("Analyzing snapshot for hub: {}", hubId);

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
            } else {
                log.debug("Scenario '{}' did not match", scenario.getName());
            }
        }
    }

    private boolean checkScenario(Scenario scenario, Map<String, SensorStateAvro> sensorsState) {
        for (ScenarioCondition sc : scenario.getConditions()) {
            String sensorId = sc.getSensor().getId();
            SensorStateAvro state = sensorsState.get(sensorId);

            if (state == null) {
                log.debug("Sensor {} not found in snapshot", sensorId);
                return false;
            }
            if (!checkCondition(sc.getCondition(), state)) {
                return false;
            }
        }
        return true;
    }

    private boolean checkCondition(Condition condition, SensorStateAvro state) {
        Integer actual = extractValue(state.getData(), condition.getType());
        Integer expected = condition.getValue();

        if (actual == null || expected == null) {
            log.debug("Cannot compare: actual={}, expected={}", actual, expected);
            return false;
        }

        boolean result = switch (condition.getOperation()) {
            case EQUALS -> actual.equals(expected);
            case GREATER_THAN -> actual > expected;
            case LOWER_THAN -> actual < expected;
        };

        log.debug("Condition check: type={}, actual={}, op={}, expected={} -> {}",
                condition.getType(), actual, condition.getOperation(), expected, result);
        return result;
    }

    private Integer extractValue(Object sensorData, ConditionType type) {
        return switch (type) {
            case TEMPERATURE -> {
                if (sensorData instanceof TemperatureSensorAvro t) {
                    yield t.getTemperatureC();
                }
                if (sensorData instanceof ClimateSensorAvro c) {
                    yield c.getTemperatureC();
                }
                yield null;
            }
            case HUMIDITY -> {
                if (sensorData instanceof ClimateSensorAvro c) {
                    yield c.getHumidity();
                }
                yield null;
            }
            case CO2LEVEL -> {
                if (sensorData instanceof ClimateSensorAvro c) {
                    yield c.getCo2Level();
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

            DeviceActionProto.Builder builder = DeviceActionProto.newBuilder()
                    .setSensorId(sensor.getId())
                    .setType(convertActionType(action.getType()));

            if (action.getValue() != null) {
                builder.setValue(action.getValue());
            }

            try {
                hubRouterClient.sendDeviceAction(hubId, scenario.getName(), builder.build());
                log.info("Device action sent: hub={}, scenario={}, sensor={}, type={}",
                        hubId, scenario.getName(), sensor.getId(), action.getType());
            } catch (Exception e) {
                log.error("Failed to execute action for scenario '{}': {}",
                        scenario.getName(), e.getMessage());
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