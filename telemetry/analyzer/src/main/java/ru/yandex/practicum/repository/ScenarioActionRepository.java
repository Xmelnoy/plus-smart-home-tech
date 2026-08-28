package ru.yandex.practicum.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import ru.yandex.practicum.model.entity.scenario.ScenarioAction;
import ru.yandex.practicum.model.entity.scenario.ScenarioActionId;

public interface ScenarioActionRepository extends JpaRepository<ScenarioAction, ScenarioActionId> {

    @Modifying
    @Query("DELETE FROM ScenarioAction sa WHERE sa.sensor.id = :sensorId")
    void deleteAllBySensorId(@Param("sensorId") String sensorId);
}
