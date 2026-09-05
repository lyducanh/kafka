package com.example.wordcount.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {

    @Value("${app.kafka.topics.input:wordcount-input}")
    private String inputTopic;

    @Value("${app.kafka.topics.output:wordcount-output}")
    private String outputTopic;

    @Value("${app.kafka.partitions:3}")
    private int partitions;

    @Value("${app.kafka.replication-factor:3}")
    private int replicationFactor;

    @Bean
    public NewTopic inputTopic() {
        return TopicBuilder.name(inputTopic)
                .partitions(partitions)
                .replicas(replicationFactor)
                .build();
    }

    @Bean
    public NewTopic outputTopic() {
        return TopicBuilder.name(outputTopic)
                .partitions(partitions)
                .replicas(replicationFactor)
                .build();
    }
}
