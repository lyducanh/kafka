package com.example.wordcount.controller;

import com.example.wordcount.listener.WordCountListener;
import com.example.wordcount.model.TextMessage;
import com.example.wordcount.model.WordCountResult;
import com.example.wordcount.service.WordCountProducerService;
import com.example.wordcount.topology.WordCountTopology;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.web.bind.annotation.*;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

@Slf4j
@RestController
@RequestMapping("/api/wordcount")
public class WordCountController {

    private final WordCountProducerService producerService;
    private final WordCountListener listener;
    private final StreamsBuilderFactoryBean streamsFactoryBean;

    public WordCountController(
            WordCountProducerService producerService,
            WordCountListener listener,
            StreamsBuilderFactoryBean streamsFactoryBean
    ) {
        this.producerService = producerService;
        this.listener = listener;
        this.streamsFactoryBean = streamsFactoryBean;
    }

    @GetMapping("/status")
    public ResponseEntity<Map<String, Object>> getStatus() {
        KafkaStreams kafkaStreams = streamsFactoryBean.getKafkaStreams();
        String streamsState = (kafkaStreams != null) ? kafkaStreams.state().name() : "NOT_INITIALIZED";

        return ResponseEntity.ok(Map.of(
                "streamsState", streamsState,
                "distinctWordsCounted", listener.getLiveCounts().size(),
                "recentUpdatesCount", listener.getRecentUpdates().size()
        ));
    }

    @PostMapping("/publish")
    public ResponseEntity<?> publishText(@RequestBody TextMessage message) {
        if (message.getText() == null || message.getText().trim().isEmpty()) {
            return ResponseEntity.badRequest().body(Map.of("error", "text is required and cannot be empty"));
        }
        producerService.publishText(message.getText());
        return ResponseEntity.ok(Map.of("status", "PUBLISHED", "text", message.getText()));
    }

    @PostMapping("/sample")
    public ResponseEntity<Map<String, Object>> publishSampleSentences() {
        Map<String, Object> result = producerService.publishSampleSentences();
        return ResponseEntity.ok(result);
    }

    @GetMapping("/counts")
    public ResponseEntity<Map<String, Object>> getLiveCounts() {
        Map<String, Long> counts = listener.getLiveCounts();
        return ResponseEntity.ok(Map.of(
                "totalDistinctWords", counts.size(),
                "counts", counts
        ));
    }

    @GetMapping("/recent")
    public ResponseEntity<List<WordCountResult>> getRecentUpdates() {
        return ResponseEntity.ok(listener.getRecentUpdates());
    }

    /**
     * Interactive Query: Directly queries the local RocksDB state store (word-count-store) for all words.
     */
    @GetMapping("/store")
    public ResponseEntity<?> queryAllFromRocksDb() {
        KafkaStreams kafkaStreams = streamsFactoryBean.getKafkaStreams();
        if (kafkaStreams == null || !kafkaStreams.state().isRunningOrRebalancing()) {
            return ResponseEntity.status(503).body(Map.of("error", "Kafka Streams is not running yet"));
        }

        try {
            ReadOnlyKeyValueStore<String, Long> store = kafkaStreams.store(
                    StoreQueryParameters.fromNameAndType(
                            WordCountTopology.WORD_COUNT_STORE,
                            QueryableStoreTypes.keyValueStore()
                    )
            );

            Map<String, Long> results = new TreeMap<>();
            try (KeyValueIterator<String, Long> iterator = store.all()) {
                while (iterator.hasNext()) {
                    var entry = iterator.next();
                    results.put(entry.key, entry.value);
                }
            }

            return ResponseEntity.ok(Map.of(
                    "storeName", WordCountTopology.WORD_COUNT_STORE,
                    "storageEngine", "RocksDB",
                    "totalWordsInStore", results.size(),
                    "words", results
            ));
        } catch (Exception e) {
            log.error("Error querying RocksDB word-count store", e);
            return ResponseEntity.internalServerError().body(Map.of("error", e.getMessage()));
        }
    }

    /**
     * Interactive Query: Query the count of a specific word directly from the local RocksDB store.
     */
    @GetMapping("/store/{word}")
    public ResponseEntity<?> queryWordFromRocksDb(@PathVariable String word) {
        KafkaStreams kafkaStreams = streamsFactoryBean.getKafkaStreams();
        if (kafkaStreams == null || !kafkaStreams.state().isRunningOrRebalancing()) {
            return ResponseEntity.status(503).body(Map.of("error", "Kafka Streams is not running yet"));
        }

        try {
            ReadOnlyKeyValueStore<String, Long> store = kafkaStreams.store(
                    StoreQueryParameters.fromNameAndType(
                            WordCountTopology.WORD_COUNT_STORE,
                            QueryableStoreTypes.keyValueStore()
                    )
            );

            Long count = store.get(word.toLowerCase());
            if (count == null) {
                return ResponseEntity.status(404).body(Map.of(
                        "word", word.toLowerCase(),
                        "found", false,
                        "message", "Word not found in RocksDB state store"
                ));
            }

            return ResponseEntity.ok(Map.of(
                    "storeName", WordCountTopology.WORD_COUNT_STORE,
                    "word", word.toLowerCase(),
                    "count", count,
                    "found", true
            ));
        } catch (Exception e) {
            log.error("Error querying word '{}' from RocksDB", word, e);
            return ResponseEntity.internalServerError().body(Map.of("error", e.getMessage()));
        }
    }
}
