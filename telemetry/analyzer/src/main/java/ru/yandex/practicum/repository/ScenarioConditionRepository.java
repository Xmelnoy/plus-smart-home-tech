package ru.yandex.practicum.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import ru.yandex.practicum.model.entity.scenario.ScenarioCondition;
import ru.yandex.practicum.model.entity.scenario.ScenarioConditionId;

public interface ScenarioConditionRepository extends JpaRepository<ScenarioCondition, ScenarioConditionId> {

    @Modifying
    @Query("DELETE FROM ScenarioCondition sc WHERE sc.sensor.id = :sensorId")
    void deleteAllBySensorId(@Param("sensorId") String sensorId);
}
