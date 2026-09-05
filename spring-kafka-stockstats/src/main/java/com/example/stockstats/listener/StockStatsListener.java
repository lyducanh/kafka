package com.example.stockstats.listener;

import com.example.stockstats.model.TradeStats;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Slf4j
@Component
public class StockStatsListener {

    private final List<TradeStats> recentStats = Collections.synchronizedList(new ArrayList<>());
    private static final int MAX_HISTORY = 50;

    @KafkaListener(
            topics = "${app.kafka.topics.stockstats-output:stockstats-output}",
            groupId = "${spring.kafka.consumer.group-id:stockstats-consumer-group}"
    )
    public void listen(ConsumerRecord<String, TradeStats> record) {
        TradeStats stats = record.value();
        log.info("[LISTENER-RECEIVED] Key='{}' Ticker='{}' Count={} AvgPrice={:.2f} Min={:.2f} Max={:.2f}",
                record.key(), stats.getTicker(), stats.getCountTrades(),
                stats.getAvgPrice(), stats.getMinPrice(), stats.getMaxPrice());

        recentStats.add(0, stats);
        if (recentStats.size() > MAX_HISTORY) {
            recentStats.remove(recentStats.size() - 1);
        }
    }

    public List<TradeStats> getRecentStats() {
        return new ArrayList<>(recentStats);
    }
}
