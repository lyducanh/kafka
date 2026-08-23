package com.example.kafka.controller;

import com.example.kafka.service.streaming.WordCountStreamTopology;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

import java.util.LinkedHashMap;
import java.util.Map;

@Slf4j
@RestController
@RequestMapping("/api/kafka/streams/word-count")
@RequiredArgsConstructor
public class KafkaStreamingController {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final StreamsBuilderFactoryBean factoryBean;

    /**
     * Publish text lines to the input topic for stream processing.
     * Example: curl -X POST http://localhost:8081/api/kafka/streams/word-count/publish \
     *   -H "Content-Type: application/json" \
     *   -d '{"text":"Kafka streams processing real time text processing!"}'
     */
    @PostMapping("/publish")
    public ResponseEntity<?> publishText(@RequestBody Map<String, String> body) {
        String text = body.get("text");
        if (text == null || text.isBlank()) {
            return ResponseEntity.badRequest().body(Map.of("error", "'text' field cannot be empty"));
        }

        try {
            var sendResult = kafkaTemplate.executeInTransaction(kt -> {
                try {
                    return kt.send(WordCountStreamTopology.TEXT_INPUT_TOPIC, text).get();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });

            log.info("[STREAMS-API] Published text to topic={} partition={} offset={}",
                    sendResult.getRecordMetadata().topic(),
                    sendResult.getRecordMetadata().partition(),
                    sendResult.getRecordMetadata().offset());

            return ResponseEntity.ok(Map.of(
                    "status", "PUBLISHED",
                    "topic", WordCountStreamTopology.TEXT_INPUT_TOPIC,
                    "text", text,
                    "partition", sendResult.getRecordMetadata().partition(),
                    "offset", sendResult.getRecordMetadata().offset()
            ));
        } catch (Exception e) {
            log.error("[STREAMS-API] Error publishing text to Kafka", e);
            return ResponseEntity.internalServerError().body(Map.of("error", e.getMessage()));
        }
    }

    /**
     * Get all word counts from the Kafka Streams state store interactively.
     * Example: curl http://localhost:8081/api/kafka/streams/word-count/stats
     */
    @GetMapping("/stats")
    public ResponseEntity<?> getAllWordCounts() {
        KafkaStreams kafkaStreams = factoryBean.getKafkaStreams();
        if (kafkaStreams == null || !kafkaStreams.state().isRunningOrRebalancing()) {
            return ResponseEntity.status(503).body(Map.of("error", "Kafka Streams is not in RUNNING state yet"));
        }

        try {
            ReadOnlyKeyValueStore<String, Long> store = kafkaStreams.store(
                    StoreQueryParameters.fromNameAndType(
                            WordCountStreamTopology.TOTAL_WORD_COUNT_STORE,
                            QueryableStoreTypes.keyValueStore()
                    )
            );

            Map<String, Long> result = new LinkedHashMap<>();
            try (KeyValueIterator<String, Long> range = store.all()) {
                while (range.hasNext()) {
                    var entry = range.next();
                    result.put(entry.key, entry.value);
                }
            }

            return ResponseEntity.ok(Map.of(
                    "totalWordsTracked", result.size(),
                    "wordCounts", result
            ));
        } catch (Exception e) {
            log.error("[STREAMS-API] Error querying state store", e);
            return ResponseEntity.internalServerError().body(Map.of("error", e.getMessage()));
        }
    }

    /**
     * Query count for a specific word from the state store.
     * Example: curl http://localhost:8081/api/kafka/streams/word-count/kafka
     */
    @GetMapping("/{word}")
    public ResponseEntity<?> getWordCount(@PathVariable String word) {
        KafkaStreams kafkaStreams = factoryBean.getKafkaStreams();
        if (kafkaStreams == null || !kafkaStreams.state().isRunningOrRebalancing()) {
            return ResponseEntity.status(503).body(Map.of("error", "Kafka Streams is not in RUNNING state yet"));
        }

        try {
            ReadOnlyKeyValueStore<String, Long> store = kafkaStreams.store(
                    StoreQueryParameters.fromNameAndType(
                            WordCountStreamTopology.TOTAL_WORD_COUNT_STORE,
                            QueryableStoreTypes.keyValueStore()
                    )
            );

            Long count = store.get(word.toLowerCase());
            if (count == null) {
                return ResponseEntity.ok(Map.of("word", word.toLowerCase(), "count", 0, "found", false));
            }

            return ResponseEntity.ok(Map.of("word", word.toLowerCase(), "count", count, "found", true));
        } catch (Exception e) {
            log.error("[STREAMS-API] Error querying word count for: " + word, e);
            return ResponseEntity.internalServerError().body(Map.of("error", e.getMessage()));
        }
    }
}
