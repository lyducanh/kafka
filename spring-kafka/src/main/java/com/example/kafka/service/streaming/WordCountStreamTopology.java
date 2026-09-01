package com.example.kafka.service.streaming;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.WindowStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Arrays;

@Slf4j
@Component
public class WordCountStreamTopology {

    public static final String TEXT_INPUT_TOPIC = "text-input-topic";
    public static final String WORD_COUNT_OUTPUT_TOPIC = "word-count-output-topic";
    public static final String TOTAL_WORD_COUNT_STORE = "word-count-total-store";
    public static final String WINDOW_WORD_COUNT_STORE = "word-count-window-store";

    @Autowired
    public void buildPipeline(StreamsBuilder streamsBuilder) {
        KStream<String, String> textLines = streamsBuilder.stream(
                TEXT_INPUT_TOPIC,
                Consumed.with(Serdes.String(), Serdes.String())
        );

        KStream<String, String> words = textLines
                .peek((key, value) -> log.info("[STREAMS-INPUT] Raw line received: {}", value))
                .flatMapValues(text -> Arrays.asList(text.toLowerCase().split("\\W+")))
                .filter((key, word) -> word != null && !word.trim().isEmpty())
                .selectKey((key, word) -> word);

        // 1. Overall cumulative Word Count (Stateful KTable)
        KTable<String, Long> wordCounts = words
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as(TOTAL_WORD_COUNT_STORE)
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.Long()));

        // Emit overall counts to output topic
        wordCounts.toStream()
                .map((word, count) -> {
                    log.info("[STREAMS-OUTPUT] Word='{}' TotalCount={}", word, count);
                    return new KeyValue<>(word, word + ":" + count);
                })
                .to(WORD_COUNT_OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

        // 2. 1-Minute Tumbling Window Word Count
        //
        // Unlike the cumulative count above (which keeps a running total forever),
        // this creates a TUMBLING WINDOW — a fixed-size, non-overlapping time bucket.
        // Each 1-minute window independently counts how many times each word appeared
        // within that specific minute. When the window closes, the count resets for the
        // next window.
        //
        // Example timeline:
        //   [09:00 - 09:01) "kafka" appeared 3 times → count = 3
        //   [09:01 - 09:02) "kafka" appeared 1 time  → count = 1  (reset, not 4)
        //
        words
                // Group all records by key (the word itself) so each word is aggregated
                // independently. Serdes specify how keys/values are serialized for the
                // internal repartition topic Kafka Streams creates under the hood.
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))

                // Apply a 1-minute tumbling window. "NoGrace" means late-arriving records
                // (events whose timestamp falls in an already-closed window) are discarded
                // rather than reopening the window. This keeps things simple but means
                // out-of-order data older than 1 minute will be dropped.
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1)))

                // Count occurrences within each window. The result is materialized into a
                // local state store (RocksDB) named WINDOW_WORD_COUNT_STORE, which makes
                // it queryable via the Interactive Queries API (e.g., from a REST endpoint).
                .count(Materialized.<String, Long, WindowStore<Bytes, byte[]>>as(WINDOW_WORD_COUNT_STORE)
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.Long()))

                // Convert the KTable (changelog stream) back to a KStream so we can
                // attach a peek() side-effect. Each emitted record has a Windowed<String>
                // key containing both the word and the window time boundaries.
                .toStream()

                // Log every windowed count update for observability. This is a side-effect
                // only — it does not modify the stream. Useful for debugging to see which
                // window a word falls into and its current count within that window.
                .peek((windowedKey, count) ->
                        log.info("[STREAMS-WINDOW] Word='{}' Window=[{} - {}] Count={}",
                                windowedKey.key(), windowedKey.window().startTime(), windowedKey.window().endTime(), count));
    }
}
