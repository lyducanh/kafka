# Spring Kafka Producer

## Publishing a Message

### Required Parameters

| Parameter | Description | Example |
|-----------|-------------|---------|
| `topic`   | Target Kafka topic name | `demo-messages` |
| `key`     | Message key (determines partition) | `"user-123"` |
| `value`   | Message payload | `"{\"content\":\"hello\"}"` |

### Available Methods

**Synchronous send** (blocks until acknowledgment):
```java
RecordMetadata metadata = producerService.send(topic, key, value);
```

**Asynchronous send** (non-blocking with callback):
```java
producerService.sendAsync(topic, key, value);
```

**Auto-generate key and value** (for load testing):
```java
Message msg = producerService.generateAndSend(topic, count);
```

## Key & Partition Strategy

### How Kafka Assigns Partitions
- if key is null(default) round robin else `partition = hash(key) % number_of_partitions  (murmur2 hash)` 
- or we can custom the key distribution partition 