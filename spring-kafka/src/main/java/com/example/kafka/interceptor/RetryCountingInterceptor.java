package com.example.kafka.interceptor;

import com.example.kafka.model.Message;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
public class RetryCountingInterceptor implements ProducerInterceptor<String, Message> {

    private static final AtomicLong totalRetries = new AtomicLong(0);
    private static final AtomicLong totalSuccess = new AtomicLong(0);

    private final Map<String, AtomicInteger> retryCount = new ConcurrentHashMap<>();

    @Override
    public ProducerRecord<String, Message> onSend(ProducerRecord<String, Message> record) {
        Message msg = record.value();
        log.info("[SEND] topic={} partition={} key={} id={}",
                record.topic(), record.partition(), record.key(),
                msg != null ? msg.getId() : "null");
        return record;
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        if (exception != null) {
            String key = buildKey(metadata);
            AtomicInteger count = retryCount.computeIfAbsent(key, k -> new AtomicInteger(0));
            int current = count.incrementAndGet();
            totalRetries.incrementAndGet();
            log.warn("[RETRY] topic={} partition={} attempt={} error={}",
                    metadata == null ? "unknown" : metadata.topic(),
                    metadata == null ? -1 : metadata.partition(),
                    current,
                    exception.getMessage());
        } else {
            String key = buildKey(metadata);
            AtomicInteger count = retryCount.remove(key);
            int attempts = count == null ? 0 : count.get();
            totalSuccess.incrementAndGet();
            log.info("[SUCCESS] topic={} partition={} offset={} retries={}",
                    metadata.topic(), metadata.partition(), metadata.offset(),
                    attempts == 0 ? "0 (first try)" : String.valueOf(attempts));
        }
    }

    private String buildKey(RecordMetadata metadata) {
        if (metadata == null) return "unknown-" + System.nanoTime();
        return metadata.topic() + "-" + metadata.partition();
    }

    @Override
    public void close() {
        retryCount.clear();
    }

    @Override
    public void configure(Map<String, ?> configs) {
    }

    public static long getTotalRetries() {
        return totalRetries.get();
    }

    public static long getTotalSuccess() {
        return totalSuccess.get();
    }

    public static void resetStats() {
        totalRetries.set(0);
        totalSuccess.set(0);
    }
}