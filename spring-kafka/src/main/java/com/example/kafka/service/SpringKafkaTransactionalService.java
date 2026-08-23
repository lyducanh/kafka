package com.example.kafka.service;

import com.example.kafka.config.KafkaConfig;
import com.example.kafka.model.Message;
import com.example.kafka.serializer.AvroKafkaSerializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.*;

@Slf4j
@Service
public class SpringKafkaTransactionalService {

    private final KafkaTemplate<String, Message> kafkaTemplate;

    public SpringKafkaTransactionalService(KafkaConfig kafkaConfig) {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConfig.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, AvroKafkaSerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

        DefaultKafkaProducerFactory<String, Message> pf = new DefaultKafkaProducerFactory<>(props);
        pf.setTransactionIdPrefix("spring-tx-");
        
        this.kafkaTemplate = new KafkaTemplate<>(pf);
        log.info("[SPRING-TX] Spring KafkaTemplate initialized with transactionIdPrefix='spring-tx-'");
    }

    /**
     * Executes a multi-topic transaction using Spring's KafkaTemplate executeInTransactionCallback.
     */
    public Map<String, Object> sendMultiTopicInTransaction(
            String topic1, String key1, String val1,
            String topic2, String key2, String val2) {

        Message msg1 = buildMessage(topic1, key1, val1);
        Message msg2 = buildMessage(topic2, key2, val2);

        log.info("[SPRING-TX][EXECUTE-IN-TX] Executing multi-topic transaction for [{}] and [{}]...", topic1, topic2);

        Boolean success = kafkaTemplate.executeInTransaction(kt -> {
            kt.send(topic1, msg1.getKey(), msg1);
            kt.send(topic2, msg2.getKey(), msg2);
            return true;
        });

        log.info("[SPRING-TX][COMMIT] Multi-topic transaction executed successfully: {}", success);

        return Map.of(
                "status", "COMMITTED",
                "framework", "Spring Kafka (executeInTransaction)",
                "topic1", topic1,
                "topic2", topic2,
                "success", Boolean.TRUE.equals(success)
        );
    }

    /**
     * Executes a multi-topic transaction ASYNCHRONOUSLY using Spring's KafkaTemplate.
     */
    public java.util.concurrent.CompletableFuture<Map<String, Object>> sendMultiTopicAsyncInTransaction(
            String topic1, String key1, String val1,
            String topic2, String key2, String val2) {

        Message msg1 = buildMessage(topic1, key1, val1);
        Message msg2 = buildMessage(topic2, key2, val2);

        return java.util.concurrent.CompletableFuture.supplyAsync(() -> {
            log.info("[SPRING-TX-ASYNC][START] Executing async multi-topic transaction for [{}] and [{}]...", topic1, topic2);

            Boolean success = kafkaTemplate.executeInTransaction(kt -> {
                var f1 = kt.send(topic1, msg1.getKey(), msg1);
                var f2 = kt.send(topic2, msg2.getKey(), msg2);

                // Await async completion of both sends inside transaction scope
                java.util.concurrent.CompletableFuture.allOf(f1, f2).join();
                return true;
            });

            log.info("[SPRING-TX-ASYNC][COMMIT] Async transaction executed successfully: {}", success);

            return Map.of(
                    "status", "COMMITTED",
                    "framework", "Spring Kafka Async (executeInTransaction)",
                    "topic1", topic1,
                    "topic2", topic2,
                    "success", Boolean.TRUE.equals(success)
            );
        });
    }

    /**
     * Demonstrates an aborted multi-topic transaction in Spring Kafka by throwing an exception
     * inside the executeInTransaction block.
     */
    public Map<String, Object> sendMultiTopicAndAbort(
            String topic1, String key1, String val1,
            String topic2, String key2, String val2) {

        Message msg1 = buildMessage(topic1, key1, val1);
        Message msg2 = buildMessage(topic2, key2, val2);

        log.info("[SPRING-TX][ABORT-DEMO] Starting multi-topic transaction for [{}] and [{}] (will throw exception)...", topic1, topic2);

        try {
            kafkaTemplate.executeInTransaction(kt -> {
                kt.send(topic1, msg1.getKey(), msg1);
                kt.send(topic2, msg2.getKey(), msg2);
                log.warn("[SPRING-TX][ABORTING] Throwing simulated RuntimeException inside executeInTransaction...");
                throw new RuntimeException("Simulated exception triggering Spring Kafka transaction rollback");
            });
        } catch (Exception e) {
            log.info("[SPRING-TX][ROLLED_BACK] Caught exception. Multi-topic transaction was rolled back automatically by Spring: {}", e.getMessage());
            return Map.of(
                    "status", "ABORTED",
                    "framework", "Spring Kafka (executeInTransaction)",
                    "topic1", topic1,
                    "topic2", topic2,
                    "reason", e.getMessage()
            );
        }

        return Map.of("status", "UNEXPECTED");
    }

    private Message buildMessage(String topic, String key, String value) {
        return Message.builder()
                .id(UUID.randomUUID().toString())
                .topic(topic)
                .key(key)
                .value(value)
                .timestamp(Instant.now())
                .build();
    }
}
