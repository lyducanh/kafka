# Kafka Producer Retry Reproduction Guide

This guide explains how to observe Kafka's internal retry mechanism using the `RetryCountingInterceptor` in a 3-broker cluster.

## Retry Behavior

When a producer send fails with a **retriable exception** (e.g. `NOT_LEADER_OR_FOLLOWER`, `LeaderNotAvailableException`), Kafka internally retries the request up to `retries` times before either succeeding or throwing to the application.

`RetryCountingInterceptor` hooks into this process:

| Log | When it fires |
|---|---|
| `[SEND]` | Record passed to Kafka producer |
| `[RETRY]` | Kafka internally retried after retriable exception |
| `[SUCCESS]` | Broker acknowledged the record |

> **Note:** Kafka's internal retries are invisible to your application code — `producer.send().get()` only sees the final result. The interceptor sees every attempt.

## Prerequisites

- Docker Compose cluster running: `docker compose up -d`
- Spring Boot app running: `cd spring-kafka && mvn spring-boot:run`
- Logs visible in the Spring Boot console

---

## Step 1: Create a Test Topic (RF=3, minISR=2)

```bash
docker exec kafka1 kafka-topics --create \
  --topic retry-test \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 3

# Set min.insync.replicas = 2 for this topic
docker exec kafka1 kafka-configs --alter \
  --topic retry-test \
  --bootstrap-server localhost:9092 \
  --add-config min.insync.replicas=2
```

## Step 2: Find the Partition Leader

```bash
docker exec kafka1 kafka-topics --describe \
  --topic retry-test \
  --bootstrap-server localhost:9092
```

Example output:
```
Topic: retry-test    Partition: 0    Leader: 2    Replicas: 2,1,3    Isr: 2,1,3
```

- **Leader 2** means `kafka2` owns the partition.
- With RF=3 and minISR=2, killing 1 broker is still fine (2 replicas remain ≥ minISR).
- Kill **2 brokers** to trigger retries — the remaining broker has the data, but no leader exists until election.

## Step 3: Send Messages (via API or Console)

**Option A — API call:**
```bash
curl -X POST http://localhost:8081/api/kafka/send \
  -H "Content-Type: application/json" \
  -d '{"topic":"retry-test","key":"test","value":"hello"}'
```

**Option B — Console producer (continuous):**
```bash
docker exec -it kafka1 bash -c '
while true; do
  echo "msg-$(date +%s)"
  sleep 1
done' | docker exec -i kafka1 kafka-console-producer \
  --topic retry-test --bootstrap-server localhost:9092
```

## Step 4: Kill the Leader Broker

```bash
# Based on step 2 — kill the broker that owns partition 0
docker stop kafka2
```

## Step 5: Observe Retry Logs

In the Spring Boot console, you'll see:

```
[SEND] topic=retry-test partition=0 key=test
[RETRY] topic=retry-test partition=0 attempt=1 error=NOT_LEADER_OR_FOLLOWER
[RETRY] topic=retry-test partition=0 attempt=2 error=NOT_LEADER_OR_FOLLOWER
[RETRY] topic=retry-test partition=0 attempt=3 error=NOT_LEADER_OR_FOLLOWER
[SUCCESS] topic=retry-test partition=0 offset=42 retries=3
```

- Kafka retries up to `retries=3` (configured in `KafkaProducerService`)
- Between each retry: `retry.backoff.ms=1000` (1 second wait)
- After retries exhausted or timeout: throws to application OR succeeds if leader re-elected

## Step 6: Restore the Broker

```bash
docker start kafka2
```

## Key Producer Settings

| Setting | Value | Location |
|---|---|---|
| `retries` | 3 | `KafkaProducerService.java:36` |
| `retry.backoff.ms` | 1000 | `KafkaProducerService.java:37` |
| `acks` | all | `KafkaProducerService.java:35` |
| `delivery.timeout.ms` | 30000 | `KafkaProducerService.java:38` |

## How to Kill 2 Brokers (Stronger Test)

With RF=3 and minISR=2:

| Brokers killed | Result |
|---|---|
| 0 | Normal operation |
| 1 | Kafka elects new leader, retries occur, succeeds |
| 2 | No leader available, retries exhaust, **application throws exception** |

```bash
# Kill leader + one follower
docker stop kafka2
docker stop kafka1

# All writes fail — no leader can be elected
# Application throws: KafkaException: Expiring 3 record(s) for retry-test-0 due to timeout

# Restore both
docker start kafka1
docker start kafka2
```

## Cleanup

```bash
# Delete the test topic
docker exec kafka1 kafka-topics --delete \
  --topic retry-test \
  --bootstrap-server localhost:9092
```

## Quick Reference: Full Flow

```
Producer.send() called
    ↓
[SEND] — interceptor.onSend()
    ↓
Kafka tries broker kafka2:9093
    ↓
kafka2 is down → retriable exception
    ↓
[RETRY] attempt=1 — interceptor.onAcknowledgement(exception)
    ↓
Kafka retries → tries broker kafka1:9093
    ↓
kafka1 has stale ISR list → retriable exception
    ↓
[RETRY] attempt=2 — interceptor.onAcknowledgement(exception)
    ↓
Kafka retries → new leader elected, ISR updated
    ↓
[SUCCESS] retries=2 — interceptor.onAcknowledgement(null)
    ↓
producer.send().get() returns RecordMetadata
```