package ru.yandex.practicum.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import deserializer.SensorsSnapshotDeserializer;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;
import ru.yandex.practicum.service.SnapshotService;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

@Slf4j
public class SnapshotConsumer implements Runnable {

    private final String bootstrapServers;
    private final String topic;
    private final String groupId;
    private final SnapshotService snapshotService;
    private volatile boolean running = true;

    public SnapshotConsumer(String bootstrapServers, String topic, String groupId, SnapshotService snapshotService) {
        this.bootstrapServers = bootstrapServers;
        this.topic = topic;
        this.groupId = groupId;
        this.snapshotService = snapshotService;
    }

    @Override
    public void run() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, SensorsSnapshotDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (KafkaConsumer<String, SensorsSnapshotAvro> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(topic));
            log.info("📸 SnapshotConsumer started and subscribed to {} (group: {})", topic, groupId);

            while (running) {
                ConsumerRecords<String, SensorsSnapshotAvro> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, SensorsSnapshotAvro> record : records) {
                    SensorsSnapshotAvro snapshot = record.value();

                    log.info("Snapshot received: hubId={}, sensors={}",
                            snapshot.getHubId(),
                            snapshot.getSensorsState().keySet());

                    snapshotService.analyzeSnapshot(snapshot);

                    consumer.commitSync();
                }
            }
        } catch (Exception e) {
            log.error("Fatal error in SnapshotConsumer", e);
        } finally {
            log.info("SnapshotConsumer stopped");
        }
    }

    public void stop() {
        running = false;
    }
}