package com.example.kafka.service;

import com.example.kafka.model.Message;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
@Service
public class KafkaConsumerService {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    private final Map<String, KafkaConsumer<String, Message>> consumers = new ConcurrentHashMap<>();
    private final Map<String, AtomicBoolean> running = new ConcurrentHashMap<>();
    private final Map<String, Long> messageCounts = new ConcurrentHashMap<>();

    public void startConsumer(String groupId, String topic) {
        startConsumer(groupId, List.of(topic), "read_committed");
    }

    public void startConsumer(String groupId, List<String> topics, String isolationLevel) {
        if (consumers.containsKey(groupId)) {
            log.warn("Consumer group '{}' already running", groupId);
            return;
        }

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "com.example.kafka.serializer.AvroKafkaDeserializer");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        if (isolationLevel != null && !isolationLevel.isBlank()) {
            props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, isolationLevel.toLowerCase());
        }

        KafkaConsumer<String, Message> consumer = new KafkaConsumer<>(props);
        consumers.put(groupId, consumer);
        running.put(groupId, new AtomicBoolean(true));
        messageCounts.put(groupId, 0L);

        consumer.subscribe(topics, new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                log.warn("[REBALANCE][{}] Partitions REVOKED: {}", groupId,
                        partitions.stream().map(tp -> tp.partition()).sorted().toList());
                if (!partitions.isEmpty()) {
                    try { consumer.commitSync(); } catch (Exception e) {}
                }
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                if (partitions.isEmpty()) {
                    log.warn("[REBALANCE][{}] No partitions assigned — consumer is idle (more consumers than partitions)", groupId);
                    return;
                }
                log.info("[REBALANCE][{}] Partitions ASSIGNED: {}",
                        groupId, partitions.stream().map(tp -> tp.partition()).sorted().toList());
            }
        });

        Thread runner = new Thread(() -> {
            log.info("[{}] Started consuming topics={} isolation.level={}", groupId, topics, isolationLevel);
            while (running.get(groupId).get()) {
                try {
                    ConsumerRecords<String, Message> records = consumer.poll(Duration.ofMillis(1000));
                    if (!records.isEmpty()) {
                        long count = messageCounts.get(groupId);
                        log.info("[CONSUME][{}] batch={} total={} topics={}",
                                groupId, records.count(), count + records.count(),
                                records.partitions().stream().map(TopicPartition::topic).distinct().toList());
                        records.forEach(r -> {
                            log.info("[RECORD][{}] topic={} key={} value={}", groupId, r.topic(), r.key(), r.value() != null ? r.value().getValue() : null);
                            messageCounts.compute(groupId, (k, v) -> v + 1);
                        });
                    }
                } catch (Exception e) {
                    if (running.get(groupId).get()) {
                        log.error("[{}] Poll error: {}", groupId, e.getMessage());
                        try { Thread.sleep(2000); } catch (InterruptedException ignored) {}
                    }
                }
            }
            log.info("[{}] Stopped", groupId);
        }, groupId + "-consumer");
        runner.setDaemon(true);
        runner.start();
    }

    public void stopConsumer(String groupId) {
        running.computeIfPresent(groupId, (k, v) -> { v.set(false); return v; });
        KafkaConsumer<String, Message> c = consumers.remove(groupId);
        if (c != null) {
            try { c.wakeup(); c.close(); } catch (Exception ignored) {}
        }
        log.info("[{}] Stopped, total messages consumed: {}", groupId, messageCounts.getOrDefault(groupId, 0L));
    }

    public Map<String, Object> getStats(String groupId) {
        return Map.of(
                "groupId", groupId,
                "running", running.getOrDefault(groupId, new AtomicBoolean(false)).get(),
                "messageCount", messageCounts.getOrDefault(groupId, 0L)
        );
    }

    public Map<String, Map<String, Object>> getAllStats() {
        Map<String, Map<String, Object>> result = new LinkedHashMap<>();
        consumers.keySet().forEach(g -> result.put(g, getStats(g)));
        return result;
    }

    public Set<String> getActiveGroups() {
        return new HashSet<>(consumers.keySet());
    }
}