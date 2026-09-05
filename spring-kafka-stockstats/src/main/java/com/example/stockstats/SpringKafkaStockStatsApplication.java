package com.example.stockstats;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@EnableScheduling
@SpringBootApplication
public class SpringKafkaStockStatsApplication {

    public static void main(String[] args) {
        SpringApplication.run(SpringKafkaStockStatsApplication.class, args);
    }
}
