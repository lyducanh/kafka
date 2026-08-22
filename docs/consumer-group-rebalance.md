# Kafka Consumer Group & Rebalance Monitoring Guide

This guide walks through creating topics, running consumer groups, and observing rebalances in real-time via Kafka UI.

## Cluster Setup

```bash
# Start fresh with clean volumes
docker compose down -v
docker compose up -d

# Verify all services are up
docker compose ps
```

Expected:
```
NAME       IMAGE                              STATUS
zookeeper  confluentinc/cp-zookeeper:6.2.0    Up
kafka1     reg-hosted.kcntt.net/.../cp-kafka  Up
kafka2     reg-hosted.kcntt.net/.../cp-kafka  Up
kafka3     reg-hosted.kcntt.net/.../cp-kafka  Up
kafka-ui   provectuslabs/kafka-ui:latest      Up
```

Kafka UI: http://localhost:8090

---

## Step 1: Create Topic with 6 Partitions and RF=3

```bash
docker exec kafka1 kafka-topics --create \
  --topic rebalance-test \
  --bootstrap-server localhost:29092 \
  --partitions 6 \
  --replication-factor 3

# Optionally set min.insync.replicas = 2
docker exec kafka1 kafka-configs --alter \
  --topic rebalance-test \
  --bootstrap-server localhost:29092 \
  --add-config min.insync.replicas=2
```

Verify:
```bash
docker exec kafka1 kafka-topics --describe \
  --topic rebalance-test \
  --bootstrap-server localhost:29092
```

Expected output — all 6 partitions have Leader on one broker, Replicas spread across all 3 brokers:
```
Topic: rebalance-test    PartitionCount: 6    ReplicationFactor: 3
Topic: rebalance-test    Partition: 0    Leader: 2    Replicas: 2,1,3    Isr: 2,1,3
Topic: rebalance-test    Partition: 1    Leader: 1    Replicas: 1,2,3    Isr: 1,2,3
Topic: rebalance-test    Partition: 2    Leader: 3    Replicas: 3,2,1    Isr: 3,2,1
Topic: rebalance-test    Partition: 3    Leader: 2    Replicas: 2,3,1    Isr: 2,3,1
Topic: rebalance-test    Partition: 4    Leader: 1    Replicas: 1,3,2    Isr: 1,3,2
Topic: rebalance-test    Partition: 5    Leader: 3    Replicas: 3,1,2    Isr: 3,1,2
```

---

## Step 2: Start Producing Messages

Keep this running in a separate terminal:

```bash
docker exec -i kafka1 bash -c '
COUNTER=0
while true; do
  echo "msg-${COUNTER}"
  COUNTER=$((COUNTER+1))
  sleep 0.5
done' | docker exec -i kafka1 kafka-console-producer \
  --topic rebalance-test \
  --bootstrap-server localhost:29092
```

This continuously produces messages with keys `msg-0`, `msg-1`, etc., so Kafka distributes them across partitions (messages with the same key always go to the same partition).

---

## Step 3: Verify Topic in Kafka UI

1. Open http://localhost:8090
2. Select **Topics** on the left sidebar
3. Click **rebalance-test**
4. You should see 6 partitions listed with their leaders and ISR

---

## Step 4: Start 2 Consumer Groups with 6 Consumers Each

Open **4 separate terminal windows** (or run in background). Each group has 6 consumers, one per partition.

### Consumer Group 1 — `group-1` (6 consumers)

**Consumer 1:**
```bash
docker exec -it kafka1 kafka-console-consumer \
  --topic rebalance-test \
  --bootstrap-server localhost:9092 \
  --group group-1 \
  --from-beginning
```

**Consumer 2-6:** Repeat the above command in separate terminals. Each consumer will be assigned 1 partition automatically.

### Consumer Group 2 — `group-2` (6 consumers)

**Consumer 1:**
```bash
docker exec -it kafka2 kafka-console-consumer \
  --topic rebalance-test \
  --bootstrap-server localhost:9092 \
  --group group-2 \
  --from-beginning
```

**Consumer 2-6:** Repeat in separate terminals.

---

## Step 5: Observe Initial Assignment in Kafka UI

1. Go to **Consumers** in the Kafka UI sidebar
2. Select **group-1** — you should see 6 consumers listed, each assigned to exactly 1 partition
3. Select **group-2** — same: 6 consumers, 1 partition each

The partition assignment is visible in the consumer detail view. Each consumer owns 1 of the 6 partitions:

```
group-1: [consumer-1 → partition 0], [consumer-2 → partition 1], ...
group-2: [consumer-1 → partition 0], [consumer-2 → partition 1], ...
```

> **Note:** Consumer IDs in Kafka console consumer are auto-generated UUIDs. In Kafka UI they appear as UUID strings. The assignment table shows which consumer owns which partition.

---

## Step 6: Monitor Rebalance — Add More Consumers

### Add 2 Consumers to group-1

Open 2 new terminal windows and run:

```bash
# New consumer A for group-1
docker exec -i kafka1 kafka-console-consumer \
  --topic rebalance-test \
  --bootstrap-server localhost:29092 \
  --group group-2 \
  --from-beginning
```

```bash
# New consumer B for group-1
docker exec -it kafka1 kafka-console-consumer \
  --topic rebalance-test \
  --bootstrap-server localhost:9092 \
  --group group-1
```

### What happens during rebalance?

- **Before:** 6 consumers × 1 partition each
- **After adding 2 consumers:** 8 consumers competing for 6 partitions
- **Result:** 6 consumers get 1 partition each, 2 consumers get **no partition** (sit idle)

**To trigger a rebalance**, each new consumer must:
1. Connect to Kafka with the same `group-id`
2. Kafka's group coordinator triggers a `JOIN_GROUP` → `SYNC_GROUP` cycle
3. Partitions are reassigned evenly across all 8 consumers

You can verify in Kafka UI:
1. Go to **Consumers** → **group-1**
2. Refresh — count increases from 6 → 8
3. Check **Partition Assignor** details — old consumers may show changed partition ownership

---

## Step 7: Trigger Rebalance via Partition Count Increase

To see a more dramatic rebalance (all consumers get reassigned), add more partitions to the topic:

```bash
docker exec kafka1 kafka-topics --alter \
  --topic rebalance-test \
  --bootstrap-server localhost:9092 \
  --partitions 12
```

**What happens:**
- Topic now has 12 partitions
- With 8 consumers in group-1: each gets 1-2 partitions (some consumers may own 2)
- With 6 consumers in group-2: 6 consumers get 2 partitions each, or 5 get 2 and 1 gets 2 (distribution depends on round-robin)
- A rebalance is triggered for both groups
- Watch Kafka UI → Consumers → group-1: assignments update immediately

---

## Step 8: Force Rebalance via Consumer Group Reset (Admin)

Trigger a rebalance without restarting consumers using `kafka-consumer-groups`:

```bash
# Show current offsets and members
docker exec kafka1 kafka-consumer-groups --describe \
  --group group-1 \
  --bootstrap-server localhost:9092

# Force rebalance by resetting offsets to earliest
docker exec kafka1 kafka-consumer-groups --reset-offsets \
  --topic rebalance-test \
  --group group-1 \
  --bootstrap-server localhost:9092 \
  --to-earliest \
  --execute
```

This causes all consumers in the group to rejoin and triggers a rebalance.

---

## Key Rebalance Concepts

| Concept | Description |
|---|---|
| **Partition Assignment** | Each consumer in a group is assigned 1+ partitions |
| **Rebalance Trigger** | Consumer joins/leaves, topic partition count changes, subscription changes |
| **Join Group** | Consumers tell the coordinator they're alive and want to join |
| **Sync Group** | Coordinator sends partition assignment to all consumers |
| **Partition Assignor** | Default is `range` or `roundrobin` — determines how partitions are distributed |
| **Sticky Assignor** | Minimizes partition movement when consumers change |
| **Owned Partition = 0** | Consumer joined the group but got no partitions (more consumers than partitions) |

---

## Quick Reference: Commands

```bash
# List all consumer groups
docker exec kafka1 kafka-consumer-groups --list --bootstrap-server localhost:9092

# Describe a consumer group (shows members, partitions, offsets, lag)
docker exec kafka1 kafka-consumer-groups --describe --group group-1 --bootstrap-server localhost:9092

# Reset offsets for a group (triggers rebalance)
docker exec kafka1 kafka-consumer-groups --reset-offsets \
  --topic rebalance-test --group group-1 \
  --bootstrap-server localhost:9092 \
  --to-earliest --execute

# Delete a consumer group
docker exec kafka1 kafka-consumer-groups --delete --group group-1 --bootstrap-server localhost:9092

# List topics
docker exec kafka1 kafka-topics --list --bootstrap-server localhost:9092

# Delete a topic
docker exec kafka1 kafka-topics --delete --topic rebalance-test --bootstrap-server localhost:9092
```

---

## Kafka UI Screenshots Guide

| Screen | What to look at |
|---|---|
| **Topics** | 6→12 partitions, RF=3 (all replicas on kafka1/2/3) |
| **Consumers** | Member count, partition assignment per member |
| **Consumer detail** | Owned partitions per consumer, lag, host |
| **Schema** | N/A (plain string messages in this demo) |
| **Brokers** | 3 brokers, leader distribution across partitions |