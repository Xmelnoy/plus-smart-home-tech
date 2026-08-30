package ru.yandex.practicum.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import deserializer.HubEventDeserializer;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.service.HubEventService;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

@Slf4j
public class HubEventConsumer implements Runnable {

    private final String bootstrapServers;
    private final String topic;
    private final String groupId;
    private final HubEventService hubEventService;
    private volatile boolean running = true;

    // ⚠️ Конструктор теперь принимает 4 параметра
    public HubEventConsumer(String bootstrapServers, String topic, String groupId, HubEventService hubEventService) {
        this.bootstrapServers = bootstrapServers;
        this.topic = topic;
        this.groupId = groupId;
        this.hubEventService = hubEventService;
    }

    @Override
    public void run() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, HubEventDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        try (KafkaConsumer<String, HubEventAvro> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(topic));
            log.info("HubEventConsumer started and subscribed to {} (group: {})", topic, groupId);

            while (running) {
                ConsumerRecords<String, HubEventAvro> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, HubEventAvro> record : records) {
                    HubEventAvro event = record.value();
                    hubEventService.handle(event);
                    consumer.commitSync();
                }
            }
        } catch (Exception e) {
            log.error("Fatal error in HubEventConsumer", e);
        } finally {
            log.info("HubEventConsumer stopped");
        }
    }

    public void stop() {
        running = false;
    }
}