package com.example.wordcount.listener;

import com.example.wordcount.model.WordCountResult;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Component
public class WordCountListener {

    private final Map<String, Long> liveCounts = new ConcurrentHashMap<>();
    private final List<WordCountResult> recentUpdates = Collections.synchronizedList(new ArrayList<>());
    private static final int MAX_HISTORY = 50;

    @KafkaListener(
            topics = "${app.kafka.topics.output:wordcount-output}",
            groupId = "${spring.kafka.consumer.group-id:wordcount-consumer-group}"
    )
    public void listen(ConsumerRecord<String, Long> record) {
        String word = record.key();
        Long count = record.value();

        if (word != null && count != null) {
            liveCounts.put(word, count);
            WordCountResult result = WordCountResult.builder()
                    .word(word)
                    .count(count)
                    .updatedAt(Instant.now())
                    .build();

            log.info("[LISTENER-RECEIVED] Word='{}' Count={}", word, count);

            recentUpdates.add(0, result);
            if (recentUpdates.size() > MAX_HISTORY) {
                recentUpdates.remove(recentUpdates.size() - 1);
            }
        }
    }

    public Map<String, Long> getLiveCounts() {
        return new TreeMap<>(liveCounts);
    }

    public List<WordCountResult> getRecentUpdates() {
        return new ArrayList<>(recentUpdates);
    }
}
