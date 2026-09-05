package com.example.wordcount.topology;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.regex.Pattern;

@Slf4j
@Component
public class WordCountTopology {

    public static final String WORD_COUNT_STORE = "word-count-store";
    private static final Pattern WORD_PATTERN = Pattern.compile("\\W+");

    @Value("${app.kafka.topics.input:wordcount-input}")
    private String inputTopic;

    @Value("${app.kafka.topics.output:wordcount-output}")
    private String outputTopic;

    @Value("${app.kafka.stop-word:the}")
    private String stopWord;

    @Autowired
    public void buildPipeline(StreamsBuilder builder) {
        // 1. Ingest raw text stream
        KStream<String, String> source = builder.stream(
                inputTopic,
                Consumed.with(Serdes.String(), Serdes.String())
        );

        source.peek((k, text) -> log.info("[STREAMS-INPUT] Raw text received: '{}'", text));

        // 2. Tokenize, clean, filter stop-word ("the"), and count
        KTable<String, Long> wordCounts = source
                .flatMapValues(value -> Arrays.asList(WORD_PATTERN.split(value.toLowerCase())))
                .filter((key, word) -> word != null && !word.trim().isEmpty())
                .filter((key, word) -> !word.equalsIgnoreCase(stopWord))
                .selectKey((key, word) -> word)
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as(WORD_COUNT_STORE)
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.Long()));

        // 3. Emit live word count changelog to output topic
        wordCounts.toStream()
                .peek((word, count) -> log.info("[STREAMS-COUNT] Word='{}' Count={}", word, count))
                .to(outputTopic, Produced.with(Serdes.String(), Serdes.Long()));
    }
}
