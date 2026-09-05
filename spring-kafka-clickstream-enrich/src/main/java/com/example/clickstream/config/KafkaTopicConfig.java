package com.example.clickstream.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {

    @Value("${app.kafka.topics.user-profile:clicks.user.profile}")
    private String userProfileTopic;

    @Value("${app.kafka.topics.page-views:clicks.pages.views}")
    private String pageViewsTopic;

    @Value("${app.kafka.topics.search:clicks.search}")
    private String searchTopic;

    @Value("${app.kafka.topics.user-activity:clicks.user.activity}")
    private String userActivityTopic;

    @Value("${app.kafka.partitions:3}")
    private int partitions;

    @Value("${app.kafka.replication-factor:3}")
    private int replicationFactor;

    @Bean
    public NewTopic userProfileTopic() {
        return TopicBuilder.name(userProfileTopic)
                .partitions(partitions)
                .replicas(replicationFactor)
                .build();
    }

    @Bean
    public NewTopic pageViewsTopic() {
        return TopicBuilder.name(pageViewsTopic)
                .partitions(partitions)
                .replicas(replicationFactor)
                .build();
    }

    @Bean
    public NewTopic searchTopic() {
        return TopicBuilder.name(searchTopic)
                .partitions(partitions)
                .replicas(replicationFactor)
                .build();
    }

    @Bean
    public NewTopic userActivityTopic() {
        return TopicBuilder.name(userActivityTopic)
                .partitions(partitions)
                .replicas(replicationFactor)
                .build();
    }
}
