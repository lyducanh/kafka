package com.example.kafka.config;

import com.example.kafka.service.KafkaConsumerService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
@ConfigurationProperties(prefix = "kafka.consumer-groups")
public class KafkaConsumerAutoStart {

    private final KafkaConsumerService consumerService;
    private final List<GroupConfig> groups = new ArrayList<>();

    public static class GroupConfig {
        private String groupId;
        private String topic;
        private boolean enabled = true;

        public String getGroupId() { return groupId; }
        public void setGroupId(String groupId) { this.groupId = groupId; }
        public String getTopic() { return topic; }
        public void setTopic(String topic) { this.topic = topic; }
        public boolean isEnabled() { return enabled; }
        public void setEnabled(boolean enabled) { this.enabled = enabled; }
    }

    public List<GroupConfig> getGroups() { return groups; }

    @EventListener(ApplicationReadyEvent.class)
    public void onReady() {
        for (GroupConfig g : groups) {
            if (g.isEnabled()) {
                log.info("Auto-starting consumer group: {} on topic: {}", g.getGroupId(), g.getTopic());
                consumerService.startConsumer(g.getGroupId(), g.getTopic());
            }
        }
    }
}