package com.example.kafka.service.streaming;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * ╔══════════════════════════════════════════════════════════════════════════╗
 * ║                     KStream vs KTable — Side by Side                   ║
 * ╠══════════════════════════════════════════════════════════════════════════╣
 * ║                                                                        ║
 * ║  Both read from the SAME topic: "user-balance-topic"                   ║
 * ║  Messages are key=userId, value=balance  (e.g. key="alice", val="100") ║
 * ║                                                                        ║
 * ║  ┌─────────────────────────────────────────────────────────────────┐    ║
 * ║  │  Produce these messages in order:                               │    ║
 * ║  │    1) alice → 100                                               │    ║
 * ║  │    2) bob   → 50                                                │    ║
 * ║  │    3) alice → 200   (alice deposits more money)                 │    ║
 * ║  │    4) alice → 150   (alice withdraws some)                      │    ║
 * ║  └─────────────────────────────────────────────────────────────────┘    ║
 * ║                                                                        ║
 * ║  KStream (event log) sees ALL 4 records:                               ║
 * ║    → alice:100, bob:50, alice:200, alice:150                           ║
 * ║    Like a bank transaction log — every event is recorded.              ║
 * ║                                                                        ║
 * ║  KTable (changelog / latest state) sees only LATEST per key:           ║
 * ║    → alice:150, bob:50                                                 ║
 * ║    Like a database table — same key = UPDATE (upsert), not INSERT.     ║
 * ║                                                                        ║
 * ╚══════════════════════════════════════════════════════════════════════════╝
 *
 * How to test:
 *   1) Create the topic:
 *      kafka-topics --create --topic user-balance-topic --partitions 1 \
 *        --replication-factor 1 --bootstrap-server localhost:9092
 *
 *   2) Start the application, then produce messages:
 *      kafka-console-producer --topic user-balance-topic \
 *        --property "parse.key=true" --property "key.separator=:" \
 *        --bootstrap-server localhost:9092
 *      > alice:100
 *      > bob:50
 *      > alice:200
 *      > alice:150
 *
 *   3) Watch the application logs to see the difference!
 *
 * ╔══════════════════════════════════════════════════════════════════════════╗
 * ║               Where does each one store data?                          ║
 * ╠══════════════════════════════════════════════════════════════════════════╣
 * ║                                                                        ║
 * ║  KStream — STATELESS by itself                                         ║
 * ║  ─────────────────────────────────                                     ║
 * ║  • KStream is just a pipe — records flow through, nothing is stored.   ║
 * ║  • Operations like peek(), filter(), map(), flatMap() are stateless.   ║
 * ║  • It does NOT maintain any local storage on its own.                   ║
 * ║                                                                        ║
 * ║  BUT: KStream becomes STATEFUL when you call:                          ║
 * ║    • groupByKey().count()      → creates a state store to hold counts  ║
 * ║    • groupByKey().aggregate()  → creates a state store for aggregates  ║
 * ║    • groupByKey().reduce()     → creates a state store for reductions  ║
 * ║    • join(otherStream)         → creates a state store for the join    ║
 * ║                                                                        ║
 * ║  These stateful operations produce a KTable internally, which is       ║
 * ║  backed by a state store. So it's not KStream that stores — it's       ║
 * ║  the KTable created by the aggregation.                                ║
 * ║                                                                        ║
 * ║  KTable — ALWAYS STATEFUL                                              ║
 * ║  ────────────────────────                                              ║
 * ║  • KTable is ALWAYS backed by a local state store (RocksDB by default) ║
 * ║  • It keeps the LATEST value for each key on local disk.               ║
 * ║  • You can query it interactively via the Interactive Queries API.     ║
 * ║                                                                        ║
 * ╠══════════════════════════════════════════════════════════════════════════╣
 * ║               Physical storage layout                                  ║
 * ╠══════════════════════════════════════════════════════════════════════════╣
 * ║                                                                        ║
 * ║  State stores live in TWO places:                                      ║
 * ║                                                                        ║
 * ║  1) LOCAL DISK (RocksDB) — for fast reads/writes                       ║
 * ║     Location: {state.dir}/{application.id}/{partition}/                ║
 * ║     In this project: ./data/kafka-streams/wordcount-streams-app/       ║
 * ║     (configured in KafkaStreamsConfig.java)                             ║
 * ║                                                                        ║
 * ║  2) KAFKA CHANGELOG TOPIC — for fault tolerance                        ║
 * ║     Topic name: {application.id}-{store-name}-changelog                ║
 * ║     Example: wordcount-streams-app-user-balance-store-changelog         ║
 * ║                                                                        ║
 * ║     Every state store write is also sent to this changelog topic.      ║
 * ║     If the app crashes, it rebuilds RocksDB by replaying the topic.   ║
 * ║                                                                        ║
 * ║  ┌─────────────┐    write     ┌──────────────────┐                     ║
 * ║  │  KTable     │ ──────────→  │  RocksDB (local) │  fast lookups       ║
 * ║  │  (in-memory │              │  ./data/kafka-    │                     ║
 * ║  │   logic)    │              │  streams/...      │                     ║
 * ║  └─────────────┘              └──────────────────┘                     ║
 * ║        │                                                               ║
 * ║        │ also writes to                                                ║
 * ║        ▼                                                               ║
 * ║  ┌──────────────────────────────────┐                                  ║
 * ║  │  Kafka changelog topic           │  fault tolerance / recovery      ║
 * ║  │  (app-id)-(store-name)-changelog │                                  ║
 * ║  └──────────────────────────────────┘                                  ║
 * ║                                                                        ║
 * ╠══════════════════════════════════════════════════════════════════════════╣
 * ║  Summary                                                               ║
 * ║  ───────                                                               ║
 * ║  • KStream alone    → STATELESS (just a pipe, no storage)              ║
 * ║  • KStream + count()/reduce()/aggregate() → STATEFUL (creates KTable) ║
 * ║  • KTable           → ALWAYS STATEFUL (RocksDB + changelog topic)     ║
 * ╚══════════════════════════════════════════════════════════════════════════╝
 */
@Slf4j
@Component
public class KStreamVsKTableTopology {

    public static final String USER_BALANCE_TOPIC = "user-balance-topic";
    public static final String USER_BALANCE_STORE = "user-balance-store";

    @Autowired
    public void buildPipeline(StreamsBuilder streamsBuilder) {

        // =====================================================================
        //  KStream — An unbounded, append-only event stream
        // =====================================================================
        //
        //  KStream treats EVERY record as an independent INSERT event.
        //  If "alice" sends 3 messages, you see all 3 — nothing is overwritten.
        //
        //  Think of it as:
        //    • A transaction log / event sourcing journal
        //    • Kafka topic records replayed one by one
        //    • Analogy: bank statement listing every deposit/withdrawal
        //
        KStream<String, String> balanceStream = streamsBuilder.stream(
                USER_BALANCE_TOPIC,
                Consumed.with(Serdes.String(), Serdes.String())
        );

        balanceStream.peek((userId, balance) ->
                log.info("[KSTREAM]  Event received → user='{}' balance={}  (every record printed, nothing overwritten)",
                        userId, balance));

        // =====================================================================
        //  KTable — A changelog stream (latest value per key)
        // =====================================================================
        // re
        //  KTable treats each record as an UPSERT (insert or update).
        //  For the same key, only the LATEST value is kept in the local state store.
        //  If "alice" sends 3 messages, only the last one survives.
        //
        //  Think of it as:
        //    • A materialized database table
        //    • A compacted view of the stream
        //    • Analogy: checking account balance — you only see the current amount
        //
        //  Under the hood, KTable is backed by a local RocksDB state store.
        //  Each new record with the same key REPLACES the previous value.
        //
        //  NOTE: We derive the KTable from the same KStream using .toTable()
        //  instead of calling streamsBuilder.table(), because Kafka Streams
        //  does NOT allow the same topic to be registered as a source twice
        //  in the same topology.
        //
        KTable<String, String> balanceTable = balanceStream.toTable(
                Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as(USER_BALANCE_STORE)
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.String())
        );

        balanceTable.toStream().peek((userId, balance) ->
                log.info("[KTABLE]   State updated → user='{}' balance={}  (only latest per key is stored)",
                        userId, balance));

        // =====================================================================
        //  Summary: What you'll see in the logs
        // =====================================================================
        //
        //  Input messages:  alice:100, bob:50, alice:200, alice:150
        //
        //  ┌───────────────────────────────────────────────────────────────┐
        //  │  [KSTREAM]  Event → user='alice'  balance=100               │
        //  │  [KTABLE]   State → user='alice'  balance=100               │
        //  │                                                             │
        //  │  [KSTREAM]  Event → user='bob'    balance=50                │
        //  │  [KTABLE]   State → user='bob'    balance=50                │
        //  │                                                             │
        //  │  [KSTREAM]  Event → user='alice'  balance=200    ← printed  │
        //  │  [KTABLE]   State → user='alice'  balance=200    ← updated  │
        //  │                                                             │
        //  │  [KSTREAM]  Event → user='alice'  balance=150    ← printed  │
        //  │  [KTABLE]   State → user='alice'  balance=150    ← updated  │
        //  └───────────────────────────────────────────────────────────────┘
        //
        //  Both print 4 log lines, BUT the critical difference is:
        //
        //  • KStream retains ALL events — if you count(), you get 3 for alice.
        //  • KTable only keeps the LAST value — state store has alice=150.
        //
        //  This matters for operations like count(), reduce(), aggregate():
        //    KStream.groupByKey().count()  →  alice=3, bob=1  (counts events)
        //    KTable.groupBy(...).count()   →  alice=1, bob=1  (counts unique keys)
        //

        // =====================================================================
        //  3. Proof: KStream.groupByKey().count() — counts every event
        // =====================================================================
        //  Groups all records by key (userId) and counts how many EVENTS arrived
        //  for each key. Since KStream is an append-only log, duplicate keys
        //  are NOT deduplicated — alice appearing 3 times means count = 3.
        //
        balanceStream
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                .count(Materialized.as("kstream-user-event-count-store"))
                .toStream()
                .peek((userId, count) ->
                        log.info("[KSTREAM-COUNT]  user='{}' eventCount={}  (counts every event, duplicates included)",
                                userId, count));

        // =====================================================================
        //  4. Proof: KTable.groupBy().count() — counts unique keys
        // =====================================================================
        //  Re-groups the KTable by the same key (userId). Since KTable only
        //  holds the LATEST value per key, grouping and counting tells you
        //  how many DISTINCT keys exist — alice is always 1, no matter how
        //  many updates she sent.
        //
        balanceTable
                .groupBy((userId, balance) -> KeyValue.pair(userId, balance),
                        Grouped.with(Serdes.String(), Serdes.String()))
                .count(Materialized.as("ktable-user-unique-count-store"))
                .toStream()
                .peek((userId, count) ->
                        log.info("[KTABLE-COUNT]   user='{}' uniqueCount={}  (counts distinct keys, latest state only)",
                                userId, count));

        // =====================================================================
        //  Expected logs after sending: alice:100, bob:50, alice:200, alice:150
        // =====================================================================
        //
        //  [KSTREAM-COUNT]  user='alice' eventCount=1
        //  [KTABLE-COUNT]   user='alice' uniqueCount=1
        //
        //  [KSTREAM-COUNT]  user='bob'   eventCount=1
        //  [KTABLE-COUNT]   user='bob'   uniqueCount=1
        //
        //  [KSTREAM-COUNT]  user='alice' eventCount=2    ← increments!
        //  [KTABLE-COUNT]   user='alice' uniqueCount=1   ← stays 1!
        //
        //  [KSTREAM-COUNT]  user='alice' eventCount=3    ← increments again!
        //  [KTABLE-COUNT]   user='alice' uniqueCount=1   ← still 1!
        //
    }
}
