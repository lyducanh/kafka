package com.example.kafka.service;

import com.example.kafka.config.KafkaConfig;
import com.example.kafka.interceptor.RetryCountingInterceptor;
import com.example.kafka.model.Message;
import com.example.kafka.serializer.AvroKafkaSerializer;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Future;

@Slf4j
@Service
public class KafkaProducerService {

    private final KafkaConfig kafkaConfig;
    private final KafkaProducer<String, Message> producer;

    public KafkaProducerService(KafkaConfig kafkaConfig) {
        this.kafkaConfig = kafkaConfig;
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConfig.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, AvroKafkaSerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, "0");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 1000);
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 30000);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "producer-1");
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip");
        props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,
                "com.example.kafka.interceptor.RetryCountingInterceptor");
        this.producer = new KafkaProducer<>(props);
    }

    public RecordMetadata send(String topic, String key, String value) throws Exception {
        return send(buildMessage(topic, key, value));
    }

    public RecordMetadata send(Message message) throws Exception {
        ProducerRecord<String, Message> record = new ProducerRecord<>(
                message.getTopic(), message.getKey(), message);
        Future<RecordMetadata> future = producer.send(record);
        RecordMetadata metadata = future.get();
        log.info("[SEND] topic={} partition={} offset={} key={}",
                message.getTopic(), metadata.partition(), metadata.offset(), message.getKey());
        return metadata;
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

    @PreDestroy
    public void close() {
        log.info("Closing KafkaProducer...");
        producer.close();
    }
}