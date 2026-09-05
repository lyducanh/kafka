package com.example.stockstats.controller;

import com.example.stockstats.listener.StockStatsListener;
import com.example.stockstats.model.Trade;
import com.example.stockstats.model.TradeStats;
import com.example.stockstats.service.StockTradeGeneratorService;
import com.example.stockstats.topology.StockStatsTopology;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.web.bind.annotation.*;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@RestController
@RequestMapping("/api/stocks")
public class StockTradeController {

    private final StockTradeGeneratorService generatorService;
    private final StockStatsListener statsListener;
    private final StreamsBuilderFactoryBean streamsFactoryBean;

    public StockTradeController(
            StockTradeGeneratorService generatorService,
            StockStatsListener statsListener,
            StreamsBuilderFactoryBean streamsFactoryBean
    ) {
        this.generatorService = generatorService;
        this.statsListener = statsListener;
        this.streamsFactoryBean = streamsFactoryBean;
    }

    @GetMapping("/status")
    public ResponseEntity<Map<String, Object>> getStatus() {
        return ResponseEntity.ok(Map.of(
                "generatorRunning", generatorService.isRunning(),
                "recentAggregationsCount", statsListener.getRecentStats().size()
        ));
    }

    @PostMapping("/generator/start")
    public ResponseEntity<Map<String, Object>> startGenerator() {
        generatorService.start();
        return ResponseEntity.ok(Map.of("message", "Trade generator started", "running", true));
    }

    @PostMapping("/generator/stop")
    public ResponseEntity<Map<String, Object>> stopGenerator() {
        generatorService.stop();
        return ResponseEntity.ok(Map.of("message", "Trade generator stopped", "running", false));
    }

    @PostMapping("/generator/toggle")
    public ResponseEntity<Map<String, Object>> toggleGenerator() {
        boolean isRunning = generatorService.toggle();
        return ResponseEntity.ok(Map.of("message", isRunning ? "Started" : "Stopped", "running", isRunning));
    }

    @PostMapping("/trade")
    public ResponseEntity<Map<String, Object>> sendTrade(@RequestBody Trade trade) {
        if (trade.getTicker() == null || trade.getPrice() <= 0) {
            return ResponseEntity.badRequest().body(Map.of("error", "Ticker and positive price are required"));
        }
        if (trade.getType() == null) {
            trade.setType("ASK");
        }
        generatorService.sendTrade(trade);
        return ResponseEntity.ok(Map.of("message", "Trade sent", "trade", trade));
    }

    @GetMapping("/stats")
    public ResponseEntity<List<TradeStats>> getRecentStats() {
        return ResponseEntity.ok(statsListener.getRecentStats());
    }

    /**
     * Query the local RocksDB window store directly for all tickers across recent windows.
     * Example: GET /api/stocks/store?minutes=30
     */
    @GetMapping("/store")
    public ResponseEntity<?> queryAllRocksDbWindows(
            @RequestParam(required = false, defaultValue = "30") int minutes
    ) {
        KafkaStreams kafkaStreams = streamsFactoryBean.getKafkaStreams();
        if (kafkaStreams == null || !kafkaStreams.state().isRunningOrRebalancing()) {
            return ResponseEntity.status(503).body(Map.of("error", "Kafka Streams is not running yet"));
        }

        try {
            ReadOnlyWindowStore<String, TradeStats> windowStore = kafkaStreams.store(
                    StoreQueryParameters.fromNameAndType(
                            StockStatsTopology.AGGREGATE_STORE,
                            QueryableStoreTypes.windowStore()
                    )
            );

            Instant to = Instant.now();
            Instant from = to.minus(Duration.ofMinutes(minutes));

            List<Map<String, Object>> results = new ArrayList<>();
            try (KeyValueIterator<Windowed<String>, TradeStats> iterator = windowStore.fetchAll(from, to)) {
                while (iterator.hasNext()) {
                    var entry = iterator.next();
                    Map<String, Object> item = new LinkedHashMap<>();
                    item.put("ticker", entry.key.key());
                    item.put("windowStart", entry.key.window().startTime().toString());
                    item.put("windowEnd", entry.key.window().endTime().toString());
                    item.put("stats", entry.value);
                    results.add(item);
                }
            }

            return ResponseEntity.ok(Map.of(
                    "storeName", StockStatsTopology.AGGREGATE_STORE,
                    "storageEngine", "RocksDB",
                    "totalWindowEntriesFound", results.size(),
                    "range", Map.of("from", from.toString(), "to", to.toString()),
                    "windows", results
            ));
        } catch (Exception e) {
            log.error("Error querying RocksDB store", e);
            return ResponseEntity.internalServerError().body(Map.of("error", e.getMessage()));
        }
    }

    /**
     * Query the local RocksDB window store for a specific ticker.
     * Example: GET /api/stocks/store/AAPL?minutes=30
     */
    @GetMapping("/store/{ticker}")
    public ResponseEntity<?> queryTickerRocksDbWindows(
            @PathVariable String ticker,
            @RequestParam(required = false, defaultValue = "30") int minutes
    ) {
        KafkaStreams kafkaStreams = streamsFactoryBean.getKafkaStreams();
        if (kafkaStreams == null || !kafkaStreams.state().isRunningOrRebalancing()) {
            return ResponseEntity.status(503).body(Map.of("error", "Kafka Streams is not running yet"));
        }

        try {
            ReadOnlyWindowStore<String, TradeStats> windowStore = kafkaStreams.store(
                    StoreQueryParameters.fromNameAndType(
                            StockStatsTopology.AGGREGATE_STORE,
                            QueryableStoreTypes.windowStore()
                    )
            );

            Instant to = Instant.now();
            Instant from = to.minus(Duration.ofMinutes(minutes));

            List<Map<String, Object>> results = new ArrayList<>();
            try (WindowStoreIterator<TradeStats> iterator = windowStore.fetch(ticker.toUpperCase(), from, to)) {
                while (iterator.hasNext()) {
                    var entry = iterator.next();
                    Map<String, Object> item = new LinkedHashMap<>();
                    item.put("ticker", ticker.toUpperCase());
                    item.put("windowTimestampMs", entry.key);
                    item.put("windowTime", Instant.ofEpochMilli(entry.key).toString());
                    item.put("stats", entry.value);
                    results.add(item);
                }
            }

            return ResponseEntity.ok(Map.of(
                    "storeName", StockStatsTopology.AGGREGATE_STORE,
                    "storageEngine", "RocksDB",
                    "ticker", ticker.toUpperCase(),
                    "windowsFound", results.size(),
                    "windows", results
            ));
        } catch (Exception e) {
            log.error("Error querying RocksDB store for ticker " + ticker, e);
            return ResponseEntity.internalServerError().body(Map.of("error", e.getMessage()));
        }
    }
}
