package com.example.kafka.listener;

import com.example.kafka.service.streaming.WordCountStreamTopology;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class WordCountResultListener {

    @KafkaListener(
            topics = WordCountStreamTopology.WORD_COUNT_OUTPUT_TOPIC,
            groupId = "word-count-listener-group"
    )
    public void listenWordCountOutput(ConsumerRecord<String, String> record) {
        log.info("[WORD-COUNT-CONSUMER] Topic={} Key='{}' Value='{}' Partition={} Offset={}",
                record.topic(), record.key(), record.value(), record.partition(), record.offset());
    }
}
