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
    private final Map<String, String> consumerToGroup = new ConcurrentHashMap<>();

    // Keep for backward compatibility
    public void startConsumer(String groupId, String topic) {
        addConsumer(groupId, topic);
    }

    public String addConsumer(String groupId, String topic) {
        String consumerId = groupId + "-" + UUID.randomUUID().toString().substring(0, 6);

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "com.example.kafka.serializer.AvroKafkaDeserializer");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);

        KafkaConsumer<String, Message> consumer = new KafkaConsumer<>(props);
        consumers.put(consumerId, consumer);
        running.put(consumerId, new AtomicBoolean(true));
        messageCounts.put(consumerId, 0L);
        consumerToGroup.put(consumerId, groupId);

        consumer.subscribe(List.of(topic), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                log.warn("[REBALANCE][{}] Partitions REVOKED: {}", consumerId,
                        partitions.stream().map(TopicPartition::partition).sorted().toList());
                if (!partitions.isEmpty()) {
                    try { consumer.commitSync(); } catch (Exception e) {}
                }
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                if (partitions.isEmpty()) {
                    log.warn("[REBALANCE][{}] No partitions assigned — consumer is idle (more consumers than partitions)", consumerId);
                    return;
                }
                log.info("[REBALANCE][{}] Partitions ASSIGNED: {}",
                        consumerId, partitions.stream().map(TopicPartition::partition).sorted().toList());
            }
        });

        Thread runner = new Thread(() -> {
            log.info("[{}] Started consuming topic={} in group={}", consumerId, topic, groupId);
            while (running.get(consumerId).get()) {
                try {
                    ConsumerRecords<String, Message> records = consumer.poll(Duration.ofMillis(1000));
                    if (!records.isEmpty()) {
                        long count = messageCounts.get(consumerId);
                        log.info("[CONSUME][{}] batch={} total={} partitions={}",
                                consumerId, records.count(), count + records.count(),
                                records.partitions().stream().map(TopicPartition::partition).sorted().toList());
                        records.forEach(r -> messageCounts.compute(consumerId, (k, v) -> v + 1));
                    }
                } catch (Exception e) {
                    if (running.get(consumerId).get()) {
                        log.error("[{}] Poll error: {}", consumerId, e.getMessage());
                        try { Thread.sleep(2000); } catch (InterruptedException ignored) {}
                    }
                }
            }
            log.info("[{}] Stopped", consumerId);
        }, consumerId + "-consumer");
        runner.setDaemon(true);
        runner.start();

        return consumerId;
    }

    public void stopConsumer(String groupId) {
        Iterator<Map.Entry<String, String>> it = consumerToGroup.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, String> entry = it.next();
            if (entry.getValue().equals(groupId)) {
                String cid = entry.getKey();
                running.computeIfPresent(cid, (k, v) -> { v.set(false); return v; });
                KafkaConsumer<String, Message> c = consumers.remove(cid);
                if (c != null) {
                    try { c.wakeup(); c.close(); } catch (Exception ignored) {}
                }
                log.info("[{}] Stopped, total messages consumed: {}", cid, messageCounts.getOrDefault(cid, 0L));
                it.remove();
                messageCounts.remove(cid);
                running.remove(cid);
            }
        }
    }

    public void stopSingleConsumer(String consumerId) {
        running.computeIfPresent(consumerId, (k, v) -> { v.set(false); return v; });
        KafkaConsumer<String, Message> c = consumers.remove(consumerId);
        if (c != null) {
            try { c.wakeup(); c.close(); } catch (Exception ignored) {}
        }
        log.info("[{}] Stopped, total messages consumed: {}", consumerId, messageCounts.getOrDefault(consumerId, 0L));
        consumerToGroup.remove(consumerId);
        messageCounts.remove(consumerId);
        running.remove(consumerId);
    }

    public Map<String, Object> getStats(String groupId) {
        long totalMessages = 0;
        int activeCount = 0;
        List<String> consumerIds = new ArrayList<>();

        for (Map.Entry<String, String> entry : consumerToGroup.entrySet()) {
            if (entry.getValue().equals(groupId)) {
                String cid = entry.getKey();
                consumerIds.add(cid);
                if (running.getOrDefault(cid, new AtomicBoolean(false)).get()) {
                    activeCount++;
                }
                totalMessages += messageCounts.getOrDefault(cid, 0L);
            }
        }

        return Map.of(
                "groupId", groupId,
                "activeConsumers", activeCount,
                "totalConsumers", consumerIds.size(),
                "consumerIds", consumerIds,
                "messageCount", totalMessages
        );
    }

    public Map<String, Map<String, Object>> getAllStats() {
        Map<String, Map<String, Object>> result = new LinkedHashMap<>();
        Set<String> groups = new HashSet<>(consumerToGroup.values());
        groups.forEach(g -> result.put(g, getStats(g)));
        return result;
    }

    public Set<String> getActiveGroups() {
        return new HashSet<>(consumerToGroup.values());
    }
}