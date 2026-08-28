package ru.yandex.practicum.model.entity;

import jakarta.persistence.*;
import lombok.*;

@Entity
@Table(name = "sensors")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Sensor {

    @Id
    @Column(name = "id", nullable = false)
    private String id;

    @Column(name = "hub_id")
    private String hubId;
}
