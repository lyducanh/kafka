package com.example.wordcount.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Slf4j
@Service
public class WordCountProducerService {

    @Value("${app.kafka.topics.input:wordcount-input}")
    private String inputTopic;

    private final KafkaTemplate<String, String> kafkaTemplate;

    public static final List<String> SAMPLE_SENTENCES = List.of(
            "The quick brown fox jumps over the lazy dog",
            "Kafka Streams is a client library for building real-time applications",
            "Apache Kafka is fast, scalable, and fault-tolerant. Kafka Streams makes stream processing simple.",
            "The streaming pipeline processes every single event, word by word, stream by stream."
    );

    public WordCountProducerService(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publishText(String text) {
        kafkaTemplate.send(inputTopic, text).whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("Failed to publish text to {}: {}", inputTopic, ex.getMessage());
            } else {
                log.info("[PRODUCED-TEXT] Topic='{}' Text='{}'", inputTopic, text);
            }
        });
    }

    public Map<String, Object> publishSampleSentences() {
        for (String sentence : SAMPLE_SENTENCES) {
            publishText(sentence);
        }
        return Map.of(
                "status", "SUCCESS",
                "sentencesPublished", SAMPLE_SENTENCES.size(),
                "sentences", SAMPLE_SENTENCES
        );
    }
}
