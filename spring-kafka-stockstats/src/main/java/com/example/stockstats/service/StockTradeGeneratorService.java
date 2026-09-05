package com.example.stockstats.service;

import com.example.stockstats.model.Trade;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
@Service
public class StockTradeGeneratorService {

    public static final String[] TICKERS = {"MMM", "ABT", "ABBV", "ACN", "ATVI", "AYI", "ADBE", "AAP", "AES", "AET"};
    private static final int START_PRICE = 5000;
    private static final int MAX_PRICE_CHANGE = 5;

    @Value("${app.kafka.topics.stocks:stocks}")
    private String stocksTopic;

    private final KafkaTemplate<String, Trade> kafkaTemplate;
    private final AtomicBoolean running;
    private final Random random = new Random();
    private final Map<String, Integer> prices = new HashMap<>();
    private long iteration = 0;

    public StockTradeGeneratorService(
            KafkaTemplate<String, Trade> kafkaTemplate,
            @Value("${app.generator.enabled:true}") boolean initialEnabled
    ) {
        this.kafkaTemplate = kafkaTemplate;
        this.running = new AtomicBoolean(initialEnabled);

        for (String ticker : TICKERS) {
            prices.put(ticker, START_PRICE);
        }
    }

    public boolean isRunning() {
        return running.get();
    }

    public void start() {
        running.set(true);
        log.info("Stock trade generator STARTED");
    }

    public void stop() {
        running.set(false);
        log.info("Stock trade generator STOPPED");
    }

    public boolean toggle() {
        boolean newState = !running.get();
        running.set(newState);
        log.info("Stock trade generator toggled: {}", newState ? "RUNNING" : "STOPPED");
        return newState;
    }

    public void sendTrade(Trade trade) {
        kafkaTemplate.send(stocksTopic, trade.getTicker(), trade);
        log.info("[PRODUCED MANUAL] {}", trade);
    }

    @Scheduled(fixedDelayString = "${app.generator.interval-ms:200}")
    public void generateTrades() {
        if (!running.get()) {
            return;
        }

        iteration++;
        for (String ticker : TICKERS) {
            double logDist = random.nextGaussian() * 0.25 + 1.0;
            int size = random.nextInt(100) + 1;
            int currentBasePrice = prices.get(ticker);

            // Fluctuate baseline price occasionally
            if (iteration % 10 == 0) {
                currentBasePrice += random.nextInt(MAX_PRICE_CHANGE * 2) - MAX_PRICE_CHANGE;
                prices.put(ticker, currentBasePrice);
            }

            double finalPrice = Math.round((currentBasePrice + logDist) * 100.0) / 100.0;
            Trade trade = Trade.builder()
                    .type("ASK")
                    .ticker(ticker)
                    .price(finalPrice)
                    .size(size)
                    .build();

            kafkaTemplate.send(stocksTopic, ticker, trade).whenComplete((result, ex) -> {
                if (ex != null) {
                    log.error("Failed to send trade for ticker {}: {}", ticker, ex.getMessage());
                }
            });
        }
    }
}
