package com.example.kafka.controller;

import com.example.kafka.service.KafkaConsumerService;
import com.example.kafka.service.SpringKafkaTransactionalService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.*;

@Slf4j
@RestController
@RequestMapping("/api/kafka/tx")
@RequiredArgsConstructor
public class KafkaTransactionController {

    private final SpringKafkaTransactionalService springTxService;
    private final KafkaConsumerService consumerService;

    private static final String DEFAULT_TOPIC_1 = "orders-topic";
    private static final String DEFAULT_TOPIC_2 = "audit-log-topic";

    /**
     * Send messages to AT LEAST 2 topics within a single transaction and COMMIT it.
     */
    @PostMapping("/commit-multi-topic")
    public ResponseEntity<?> commitMultiTopic(@RequestBody Map<String, Object> body) {
        String topic1 = (String) body.getOrDefault("topic1", DEFAULT_TOPIC_1);
        String key1 = (String) body.getOrDefault("key1", "order-key-" + System.currentTimeMillis());
        String val1 = (String) body.getOrDefault("val1", "Order Created #" + System.currentTimeMillis());

        String topic2 = (String) body.getOrDefault("topic2", DEFAULT_TOPIC_2);
        String key2 = (String) body.getOrDefault("key2", "audit-key-" + System.currentTimeMillis());
        String val2 = (String) body.getOrDefault("val2", "Audit Log for Order #" + System.currentTimeMillis());

        try {
            var res = springTxService.sendMultiTopicInTransaction(topic1, key1, val1, topic2, key2, val2);
            return ResponseEntity.ok(res);
        } catch (Exception e) {
            log.error("[TX-API] Error committing multi-topic transaction", e);
            return ResponseEntity.internalServerError().body(Map.of("status", "FAILED", "error", e.getMessage()));
        }
    }

    /**
     * ASYNCHRONOUSLY send messages to AT LEAST 2 topics within a single transaction and COMMIT it.
     */
    @PostMapping("/commit-multi-topic-async")
    public ResponseEntity<?> commitMultiTopicAsync(@RequestBody Map<String, Object> body) {
        String topic1 = (String) body.getOrDefault("topic1", DEFAULT_TOPIC_1);
        String key1 = (String) body.getOrDefault("key1", "async-order-key-" + System.currentTimeMillis());
        String val1 = (String) body.getOrDefault("val1", "Async Order Created #" + System.currentTimeMillis());

        String topic2 = (String) body.getOrDefault("topic2", DEFAULT_TOPIC_2);
        String key2 = (String) body.getOrDefault("key2", "async-audit-key-" + System.currentTimeMillis());
        String val2 = (String) body.getOrDefault("val2", "Async Audit Log #" + System.currentTimeMillis());

        try {
            var future = springTxService.sendMultiTopicAsyncInTransaction(topic1, key1, val1, topic2, key2, val2);
            return ResponseEntity.ok(future.join());
        } catch (Exception e) {
            log.error("[TX-API-ASYNC] Error committing async multi-topic transaction", e);
            return ResponseEntity.internalServerError().body(Map.of("status", "FAILED", "error", e.getMessage()));
        }
    }

    /**
     * Send messages to AT LEAST 2 topics within a single transaction and explicitly ABORT it.
     * Demonstrates that read_committed consumers will NOT see messages on either topic.
     */
    @PostMapping("/abort-multi-topic")
    public ResponseEntity<?> abortMultiTopic(@RequestBody Map<String, Object> body) {
        String topic1 = (String) body.getOrDefault("topic1", DEFAULT_TOPIC_1);
        String key1 = (String) body.getOrDefault("key1", "order-abort-key");
        String val1 = (String) body.getOrDefault("val1", "Aborted Order Data");

        String topic2 = (String) body.getOrDefault("topic2", DEFAULT_TOPIC_2);
        String key2 = (String) body.getOrDefault("key2", "audit-abort-key");
        String val2 = (String) body.getOrDefault("val2", "Aborted Audit Data");

        var res = springTxService.sendMultiTopicAndAbort(topic1, key1, val1, topic2, key2, val2);
        return ResponseEntity.ok(res);
    }

    /**
     * Start a consumer assigned to specified topics with configurable isolation level (read_committed vs read_uncommitted).
     */
    @PostMapping("/consumer/start")
    public ResponseEntity<?> startConsumer(
            @RequestParam String groupId,
            @RequestParam(defaultValue = DEFAULT_TOPIC_1 + "," + DEFAULT_TOPIC_2) String topics,
            @RequestParam(defaultValue = "read_committed") String isolationLevel) {

        List<String> topicList = Arrays.stream(topics.split(",")).map(String::trim).toList();
        consumerService.startConsumer(groupId, topicList, isolationLevel);

        return ResponseEntity.ok(Map.of(
                "status", "started",
                "groupId", groupId,
                "topics", topicList,
                "isolationLevel", isolationLevel
        ));
    }
}
