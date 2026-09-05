package com.example.clickstream;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@EnableScheduling
@SpringBootApplication
public class SpringKafkaClickstreamEnrichApplication {

    public static void main(String[] args) {
        SpringApplication.run(SpringKafkaClickstreamEnrichApplication.class, args);
    }
}
