package com.example.stockstats.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {

    @Value("${app.kafka.topics.stocks:stocks}")
    private String stocksTopic;

    @Value("${app.kafka.topics.stockstats-output:stockstats-output}")
    private String stockstatsOutputTopic;

    @Value("${app.kafka.partitions:3}")
    private int partitions;

    @Value("${app.kafka.replication-factor:3}")
    private int replicationFactor;

    @Bean
    public NewTopic stocksTopic() {
        return TopicBuilder.name(stocksTopic)
                .partitions(partitions)
                .replicas(replicationFactor)
                .build();
    }

    @Bean
    public NewTopic stockstatsOutputTopic() {
        return TopicBuilder.name(stockstatsOutputTopic)
                .partitions(partitions)
                .replicas(replicationFactor)
                .build();
    }
}
