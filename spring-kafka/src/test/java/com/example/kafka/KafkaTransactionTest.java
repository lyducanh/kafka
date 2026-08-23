package com.example.kafka;

import com.example.kafka.model.Message;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class KafkaTransactionTest {

    @Test
    @DisplayName("Verify Message structure for transactional payload")
    void testMessageBuilder() {
        Message msg1 = Message.builder()
                .id(UUID.randomUUID().toString())
                .topic("orders-topic")
                .key("order-1")
                .value("Order Created #1")
                .timestamp(Instant.now())
                .build();

        Message msg2 = Message.builder()
                .id(UUID.randomUUID().toString())
                .topic("audit-log-topic")
                .key("audit-1")
                .value("Audit Log for Order #1")
                .timestamp(Instant.now())
                .build();

        assertNotNull(msg1.getId());
        assertEquals("orders-topic", msg1.getTopic());
        assertEquals("order-1", msg1.getKey());

        assertNotNull(msg2.getId());
        assertEquals("audit-log-topic", msg2.getTopic());
        assertEquals("audit-1", msg2.getKey());
    }
}
