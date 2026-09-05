package com.example.wordcount.topology;

import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

public class WordCountTopologyTest {

    private TopologyTestDriver testDriver;
    private TestInputTopic<String, String> inputTopic;
    private TestOutputTopic<String, Long> outputTopic;

    @BeforeEach
    void setUp() {
        StreamsBuilder builder = new StreamsBuilder();
        WordCountTopology topology = new WordCountTopology();

        ReflectionTestUtils.setField(topology, "inputTopic", "wordcount-input");
        ReflectionTestUtils.setField(topology, "outputTopic", "wordcount-output");
        ReflectionTestUtils.setField(topology, "stopWord", "the");

        topology.buildPipeline(builder);

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-wordcount");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        testDriver = new TopologyTestDriver(builder.build(), props);

        inputTopic = testDriver.createInputTopic(
                "wordcount-input",
                new StringSerializer(),
                new StringSerializer()
        );

        outputTopic = testDriver.createOutputTopic(
                "wordcount-output",
                new StringDeserializer(),
                new LongDeserializer()
        );
    }

    @AfterEach
    void tearDown() {
        if (testDriver != null) {
            testDriver.close();
        }
    }

    @Test
    void testWordCountAndStopWordFiltering() {
        // "the" should be filtered out
        // "kafka" appears twice
        inputTopic.pipeInput("The Kafka streams processing with Kafka!");

        KeyValueStore<String, Long> store = testDriver.getKeyValueStore(WordCountTopology.WORD_COUNT_STORE);

        // Assert that "the" was filtered out completely
        assertNull(store.get("the"));

        // Assert counts
        assertEquals(2L, store.get("kafka"));
        assertEquals(1L, store.get("streams"));
        assertEquals(1L, store.get("processing"));
        assertEquals(1L, store.get("with"));

        // Assert output records produced to output topic
        assertFalse(outputTopic.isEmpty());
        var records = outputTopic.readKeyValuesToList();
        assertTrue(records.stream().anyMatch(kv -> kv.key.equals("kafka") && kv.value == 2L));
        assertTrue(records.stream().noneMatch(kv -> kv.key.equals("the")));
    }
}
