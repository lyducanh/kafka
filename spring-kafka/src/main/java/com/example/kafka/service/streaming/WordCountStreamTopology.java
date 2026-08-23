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
        words.groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1)))
                .count(Materialized.<String, Long, WindowStore<Bytes, byte[]>>as(WINDOW_WORD_COUNT_STORE)
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.Long()))
                .toStream()
                .peek((windowedKey, count) ->
                        log.info("[STREAMS-WINDOW] Word='{}' Window=[{} - {}] Count={}",
                                windowedKey.key(), windowedKey.window().startTime(), windowedKey.window().endTime(), count));
    }
}
