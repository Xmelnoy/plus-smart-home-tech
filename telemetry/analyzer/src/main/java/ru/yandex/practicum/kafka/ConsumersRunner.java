package ru.yandex.practicum.kafka;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.concurrent.CountDownLatch;

@Component
@Slf4j
public class ConsumersRunner {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${kafka.topics.snapshots}")
    private String snapshotsTopic;

    @Value("${kafka.topics.hubs}")
    private String hubsTopic;

    @Value("${kafka.consumer.snapshot.group-id}")
    private String snapshotGroupId;

    @Value("${kafka.consumer.hub.group-id}")
    private String hubGroupId;

    private SnapshotConsumer snapshotConsumer;
    private HubEventConsumer hubEventConsumer;
    private final CountDownLatch shutdownLatch = new CountDownLatch(1);

    @PostConstruct
    public void start() {
        log.info("Starting Kafka consumers...");

        snapshotConsumer = new SnapshotConsumer(bootstrapServers, snapshotsTopic, snapshotGroupId);
        hubEventConsumer = new HubEventConsumer(bootstrapServers, hubsTopic, hubGroupId);

        Thread.ofVirtual().name("snapshot-thread").start(snapshotConsumer);
        Thread.ofVirtual().name("hub-thread").start(hubEventConsumer);

        log.info("Kafka consumers started successfully");

        new Thread(() -> {
            try {
                shutdownLatch.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, "main-keeper").start();

    }

    @PreDestroy
    public void stop() {
        log.info("Stopping Kafka consumers...");
        if (snapshotConsumer != null) snapshotConsumer.stop();
        if (hubEventConsumer != null) hubEventConsumer.stop();
    }
}
