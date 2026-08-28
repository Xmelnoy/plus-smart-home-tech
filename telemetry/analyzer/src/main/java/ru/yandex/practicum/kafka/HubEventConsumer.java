package ru.yandex.practicum.kafka;


import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

@Slf4j
public class HubEventConsumer  implements Runnable {

    private final String bootstrapServers;
    private final String topic;
    private final String groupId;
    private volatile boolean running = true;

    public HubEventConsumer(String bootstrapServers, String topic, String groupId) {
        this.bootstrapServers = bootstrapServers;
        this.topic = topic;
        this.groupId = groupId;
    }

    @Override
    public void run() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "analyzer-hub");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        try (KafkaConsumer<String, HubEventAvro> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(topic));
            log.info("HubEventConsumer started and subscribed to {} (groupId={}", topic, groupId);

            while (running) {
                ConsumerRecords<String, HubEventAvro> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, HubEventAvro> record : records) {
                    HubEventAvro eventAvro = record.value();
                    log.info("Hub event received: hubId={}, payloadType={}",
                            eventAvro.getHubId(),
                            eventAvro.getPayload().getClass().getSimpleName());

                    consumer.commitSync();
                }
            }
        } catch (Exception e) {
            log.error("Fatal errors in HybEventConsumer", e);
        } finally {
            log.info("HubEventConsumer stopped");
        }
    }

    public void stop() {
        running = false;
    }

}
