package com.example.kafka.controller;

import com.example.kafka.model.Message;
import com.example.kafka.service.KafkaConsumerService;
import com.example.kafka.service.KafkaProducerService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@Slf4j
@RestController
@RequestMapping("/api/kafka")
@RequiredArgsConstructor
public class KafkaController {

    private final KafkaProducerService producerService;
    private final KafkaConsumerService consumerService;

    private static final String DEFAULT_TOPIC = "rebalance-test";

    // Send a message
    // curl -X POST http://localhost:8081/api/kafka/send \
    //   -H "Content-Type: application/json" \
    //   -d '{"topic":"rebalance-test","key":"key1","value":"hello"}'
    @PostMapping("/send")
    public ResponseEntity<?> send(@RequestBody Map<String, String> body) {
        String topic = body.getOrDefault("topic", DEFAULT_TOPIC);
        try {
            var md = producerService.send(topic, body.getOrDefault("key", ""), body.getOrDefault("value", ""));
            return ResponseEntity.ok(Map.of("sent", true, "partition", md.partition(), "offset", md.offset()));
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body(Map.of("error", e.getMessage()));
        }
    }

    // Start a consumer group on a topic
    // curl -X POST "http://localhost:8081/api/kafka/consumer/start?groupId=group-1&topic=rebalance-test"
    @PostMapping("/consumer/start")
    public ResponseEntity<?> startConsumer(
            @RequestParam String groupId,
            @RequestParam(defaultValue = DEFAULT_TOPIC) String topic) {
        consumerService.startConsumer(groupId, topic);
        return ResponseEntity.ok(Map.of("status", "started", "groupId", groupId, "topic", topic,
                "tip", "Watch logs for [REBALANCE] messages to see partition assignment"));
    }

    // Stop a consumer group
    // curl -X POST "http://localhost:8081/api/kafka/consumer/stop?groupId=group-1"
    @PostMapping("/consumer/stop")
    public ResponseEntity<?> stopConsumer(@RequestParam String groupId) {
        consumerService.stopConsumer(groupId);
        return ResponseEntity.ok(Map.of("status", "stopped", "groupId", groupId));
    }

    // List all active consumer groups and their message counts
    // curl http://localhost:8081/api/kafka/consumer/groups
    @GetMapping("/consumer/groups")
    public ResponseEntity<Map<String, Map<String, Object>>> listGroups() {
        return ResponseEntity.ok(consumerService.getAllStats());
    }

    @GetMapping("/health")
    public ResponseEntity<Map<String, String>> health() {
        return ResponseEntity.ok(Map.of("status", "UP"));
    }
}