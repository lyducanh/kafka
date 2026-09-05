package com.example.stockstats.topology;

import com.example.stockstats.model.Trade;
import com.example.stockstats.model.TradeStats;
import com.example.stockstats.serde.JsonSerde;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.WindowStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;

@Slf4j
@Component
public class StockStatsTopology {

    @Value("${app.kafka.topics.stocks:stocks}")
    private String stocksTopic;

    @Value("${app.kafka.topics.stockstats-output:stockstats-output}")
    private String stockstatsOutputTopic;

    public static final String AGGREGATE_STORE = "trade-aggregates";

    @Autowired
    public void buildPipeline(StreamsBuilder streamsBuilder) {
        JsonSerde<Trade> tradeSerde = new JsonSerde<>(Trade.class);
        JsonSerde<TradeStats> tradeStatsSerde = new JsonSerde<>(TradeStats.class);

        // 1. Read the stream of trade events from the 'stocks' topic
        KStream<String, Trade> trades = streamsBuilder.stream(
                stocksTopic,
                Consumed.with(Serdes.String(), tradeSerde)
        );

        trades.peek((ticker, trade) ->
                log.info("[STREAMS-IN] Received Trade: Ticker={} Price={} Size={}", ticker, trade.getPrice(), trade.getSize())
        );

        // 2. 5-Second Hopping Window advancing every 1 second
        long windowSizeMs = 5000L;
        long advanceSizeMs = 1000L;

        TimeWindows hoppingWindow = TimeWindows
                .ofSizeWithNoGrace(Duration.ofMillis(windowSizeMs))
                .advanceBy(Duration.ofMillis(advanceSizeMs));

        // 3. Group by stock ticker and aggregate trade statistics
        KGroupedStream<String, Trade> statsStream = trades
                .groupByKey(Grouped.with(Serdes.String(), tradeSerde));
//                .windowedBy(hoppingWindow)
//                .aggregate(
//                        TradeStats::new,
//                        (ticker, trade, agg) -> agg.add(trade),
//                        Materialized.<String, TradeStats, WindowStore<Bytes, byte[]>>as(AGGREGATE_STORE)
//                                .withKeySerde(Serdes.String())
//                                .withValueSerde(tradeStatsSerde)
//                )
//                .toStream()
//                .map((windowedKey, agg) -> {
//                    agg.computeAvgPrice();
//                    Instant start = windowedKey.window().startTime();
//                    Instant end = windowedKey.window().endTime();
//                    agg.setWindowStart(start.toString());
//                    agg.setWindowEnd(end.toString());
//
//                    String outputKey = String.format("%s@%d-%d",
//                            windowedKey.key(), start.toEpochMilli(), end.toEpochMilli());
//
//                    log.info("[STREAMS-WINDOW-OUTPUT] Ticker='{}' Count={} Min={:.2f} Avg={:.2f} Max={:.2f} Window=[{} -> {}]",
//                            agg.getTicker(), agg.getCountTrades(), agg.getMinPrice(),
//                            agg.getAvgPrice(), agg.getMaxPrice(), agg.getWindowStart(), agg.getWindowEnd());
//
//                    return new KeyValue<>(outputKey, agg);
//                });
//
//        // 4. Output the aggregated statistics to the 'stockstats-output' topic
//        statsStream.to(
//                stockstatsOutputTopic,
//                Produced.with(Serdes.String(), tradeStatsSerde)
//        );
    }
}
