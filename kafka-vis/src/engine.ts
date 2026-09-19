// ============================================================
// Kafka Simulation Engine
// ============================================================

import type {
  Message,
  Partition,
  Broker,
  Consumer,
  ConsumerGroup,
  ProducerConfig,
  ClusterConfig,
  KafkaState,
} from './types';

export const CONSUMER_COLORS: string[] = [
  '#4CAF50',
  '#2196F3',
  '#FF9800',
  '#9C27B0',
  '#F44336',
  '#00BCD4',
  '#795548',
  '#607D8B',
];

// ---- Helpers ------------------------------------------------

/** Simple hash (murmur-inspired) that always returns a non-negative integer. */
function hashKey(key: string): number {
  let h = 0x811c9dc5; // FNV offset basis
  for (let i = 0; i < key.length; i++) {
    h ^= key.charCodeAt(i);
    h = Math.imul(h, 0x01000193); // FNV prime
  }
  return h >>> 0; // ensure unsigned
}

function deepClonePartitions(partitions: Partition[]): Partition[] {
  return partitions.map((p) => ({
    ...p,
    messages: p.messages.map((m) => ({ ...m })),
    replicaIds: [...p.replicaIds],
    isrIds: [...p.isrIds],
  }));
}

function deepCloneBrokers(brokers: Broker[]): Broker[] {
  return brokers.map((b) => ({
    ...b,
    partitionLeaders: [...b.partitionLeaders],
    partitionReplicas: [...b.partitionReplicas],
  }));
}

function deepCloneConsumerGroups(groups: ConsumerGroup[]): ConsumerGroup[] {
  return groups.map((g) => ({
    ...g,
    consumers: g.consumers.map((c) => ({
      ...c,
      assignedPartitions: [...c.assignedPartitions],
      committedOffsets: new Map(c.committedOffsets),
      currentOffsets: new Map(c.currentOffsets),
    })),
  }));
}

// ---- Engine -------------------------------------------------

export class KafkaEngine {
  private brokers: Broker[] = [];
  private partitions: Partition[] = [];
  private consumerGroups: ConsumerGroup[] = [];
  private producerConfig: ProducerConfig = {
    produceInterval: 1000,
    acks: 'all',
  };
  private clusterConfig: ClusterConfig = {
    numBrokers: 3,
    numPartitions: 3,
    replicationFactor: 2,
    minInsyncReplicas: 1,
  };
  private isRunning = false;
  private totalMessagesProduced = 0;

  /** Auto-incrementing ID for messages. */
  private messageCounter = 0;
  /** Auto-incrementing ID for consumers. */
  private consumerCounter = 0;
  /** Auto-incrementing color index for consumer groups. */
  private groupColorIndex = 0;

  constructor(config?: ClusterConfig) {
    if (config) {
      this.initCluster(config);
    }
  }

  /** Set the producer acks configuration. */
  setAcks(acks: 'all' | '1' | '0'): void {
    this.producerConfig.acks = acks;
  }

  // -----------------------------------------------------------
  // 1.  Cluster Initialisation
  // -----------------------------------------------------------

  /** Initialise (or re-initialise) the cluster from a config. */
  initCluster(config: ClusterConfig): void {
    this.clusterConfig = { ...config };
    this.brokers = [];
    this.partitions = [];
    this.consumerGroups = [];
    this.totalMessagesProduced = 0;
    this.messageCounter = 0;
    this.consumerCounter = 0;
    this.groupColorIndex = 0;

    const { numBrokers, numPartitions, replicationFactor } = config;
    const effectiveReplication = Math.min(replicationFactor, numBrokers);

    // Create brokers
    for (let i = 0; i < numBrokers; i++) {
      this.brokers.push({
        id: i,
        isAlive: true,
        partitionLeaders: [],
        partitionReplicas: [],
      });
    }

    // Create partitions and distribute via round-robin
    for (let p = 0; p < numPartitions; p++) {
      const replicaIds: number[] = [];
      for (let r = 0; r < effectiveReplication; r++) {
        const brokerId = (p + r) % numBrokers;
        replicaIds.push(brokerId);
      }

      const leaderId = replicaIds[0];

      this.partitions.push({
        id: p,
        messages: [],
        leaderId,
        replicaIds: [...replicaIds],
        isrIds: [...replicaIds],
        nextOffset: 0,
      });

      // Update broker bookkeeping
      this.brokers[leaderId].partitionLeaders.push(p);
      for (const bId of replicaIds) {
        this.brokers[bId].partitionReplicas.push(p);
      }
    }
  }

  // -----------------------------------------------------------
  // 2.  Message Production
  // -----------------------------------------------------------

  /**
   * Produce a message.
   * Returns the produced Message, or `null` when production is rejected
   * (leader down / insufficient ISR).
   */
  produce(key: string, value: string): Message | null {
    const partitionIndex = hashKey(key) % this.partitions.length;
    const partition = this.partitions[partitionIndex];

    // Leader must be alive
    const leaderBroker = this.brokers.find((b) => b.id === partition.leaderId);
    if (!leaderBroker || !leaderBroker.isAlive) {
      return null; // partition unavailable
    }

    // When acks='all', ISR must satisfy minInsyncReplicas
    if (
      this.producerConfig.acks === 'all' &&
      partition.isrIds.length < this.clusterConfig.minInsyncReplicas
    ) {
      return null;
    }

    const message: Message = {
      id: this.messageCounter++,
      key,
      value,
      partition: partitionIndex,
      offset: partition.nextOffset++,
      timestamp: Date.now(),
    };

    partition.messages.push(message);
    this.totalMessagesProduced++;

    return message;
  }

  // -----------------------------------------------------------
  // 3.  Broker Management
  // -----------------------------------------------------------

  /** Toggle a broker on ↔ off, adjusting leaders / ISRs accordingly. */
  toggleBroker(brokerId: number): void {
    const broker = this.brokers.find((b) => b.id === brokerId);
    if (!broker) return;

    broker.isAlive = !broker.isAlive;

    if (!broker.isAlive) {
      this.handleBrokerDown(broker);
    } else {
      this.handleBrokerUp(broker);
    }
  }

  private handleBrokerDown(broker: Broker): void {
    for (const partition of this.partitions) {
      // Remove from ISR
      partition.isrIds = partition.isrIds.filter((id) => id !== broker.id);

      // Reassign leadership if this broker was leader
      if (partition.leaderId === broker.id) {
        const newLeader = partition.isrIds.find(
          (id) => this.brokers[id]?.isAlive,
        );

        // Remove from old leader bookkeeping
        broker.partitionLeaders = broker.partitionLeaders.filter(
          (pid) => pid !== partition.id,
        );

        if (newLeader !== undefined) {
          partition.leaderId = newLeader;
          if (!this.brokers[newLeader].partitionLeaders.includes(partition.id)) {
            this.brokers[newLeader].partitionLeaders.push(partition.id);
          }
        } else {
          // No alive replica → mark as unavailable with leaderId = -1
          partition.leaderId = -1;
        }
      }
    }
  }

  private handleBrokerUp(broker: Broker): void {
    for (const partition of this.partitions) {
      // If this broker is supposed to hold a replica, rejoin ISR
      if (partition.replicaIds.includes(broker.id)) {
        if (!partition.isrIds.includes(broker.id)) {
          partition.isrIds.push(broker.id);
        }

        // If partition had no leader, assign this broker
        if (
          partition.leaderId === -1 ||
          !this.brokers[partition.leaderId]?.isAlive
        ) {
          // Remove stale leader bookkeeping (if any)
          if (partition.leaderId !== -1) {
            const oldBroker = this.brokers[partition.leaderId];
            if (oldBroker) {
              oldBroker.partitionLeaders = oldBroker.partitionLeaders.filter(
                (pid) => pid !== partition.id,
              );
            }
          }

          partition.leaderId = broker.id;
          if (!broker.partitionLeaders.includes(partition.id)) {
            broker.partitionLeaders.push(partition.id);
          }
        }
      }
    }
  }

  // -----------------------------------------------------------
  // 4.  Consumer Group Management
  // -----------------------------------------------------------

  /**
   * Add a consumer to a group. Creates the group if it doesn't exist.
   * Returns the newly created Consumer.
   */
  addConsumer(groupId: string): Consumer {
    let group = this.consumerGroups.find((g) => g.id === groupId);

    if (!group) {
      const color =
        CONSUMER_COLORS[this.groupColorIndex % CONSUMER_COLORS.length];
      this.groupColorIndex++;
      group = { id: groupId, consumers: [], color };
      this.consumerGroups.push(group);
    }

    const consumer: Consumer = {
      id: `consumer-${++this.consumerCounter}`,
      groupId,
      assignedPartitions: [],
      committedOffsets: new Map(),
      currentOffsets: new Map(),
      isActive: true,
      consumeInterval: 1000,
      commitInterval: 5000,
      color: group.color,
    };

    group.consumers.push(consumer);
    this.rebalance(groupId);

    return consumer;
  }

  /** Remove a consumer from a group. */
  removeConsumer(groupId: string, consumerId: string): void {
    const group = this.consumerGroups.find((g) => g.id === groupId);
    if (!group) return;

    group.consumers = group.consumers.filter((c) => c.id !== consumerId);

    if (group.consumers.length === 0) {
      // Remove empty group
      this.consumerGroups = this.consumerGroups.filter(
        (g) => g.id !== groupId,
      );
    } else {
      this.rebalance(groupId);
    }
  }

  /**
   * Redistribute partitions among consumers using **range assignment**.
   *
   * Range strategy: sort partitions and consumers, divide partitions as
   * evenly as possible (first N consumers get one extra partition when
   * the count doesn't divide evenly).
   */
  rebalance(groupId: string): void {
    const group = this.consumerGroups.find((g) => g.id === groupId);
    if (!group || group.consumers.length === 0) return;

    const activeConsumers = group.consumers.filter((c) => c.isActive);
    if (activeConsumers.length === 0) return;

    // Sort consumers deterministically by id
    activeConsumers.sort((a, b) => a.id.localeCompare(b.id));

    const allPartitionIds = this.partitions.map((p) => p.id).sort((a, b) => a - b);
    const numPartitions = allPartitionIds.length;
    const numConsumers = activeConsumers.length;

    // Clear current assignments
    for (const consumer of group.consumers) {
      consumer.assignedPartitions = [];
    }

    // Range assignment
    const partitionsPerConsumer = Math.floor(numPartitions / numConsumers);
    const extra = numPartitions % numConsumers;
    let idx = 0;

    for (let i = 0; i < numConsumers; i++) {
      const count = partitionsPerConsumer + (i < extra ? 1 : 0);
      const assigned = allPartitionIds.slice(idx, idx + count);
      activeConsumers[i].assignedPartitions = assigned;

      // Initialise offsets for newly assigned partitions
      for (const pid of assigned) {
        if (!activeConsumers[i].currentOffsets.has(pid)) {
          const committed = activeConsumers[i].committedOffsets.get(pid);
          activeConsumers[i].currentOffsets.set(pid, committed ?? 0);
        }
      }

      idx += count;
    }
  }

  /**
   * Consume the next available message from one of the consumer's assigned
   * partitions (round-robin across assigned partitions).
   *
   * Returns the consumed Message, or `null` if nothing is available.
   */
  consume(consumerId: string, groupId: string): Message | null {
    const group = this.consumerGroups.find((g) => g.id === groupId);
    if (!group) return null;

    const consumer = group.consumers.find((c) => c.id === consumerId);
    if (!consumer || !consumer.isActive) return null;

    // Try each assigned partition in order
    for (const pid of consumer.assignedPartitions) {
      const partition = this.partitions.find((p) => p.id === pid);
      if (!partition) continue;

      const currentOffset = consumer.currentOffsets.get(pid) ?? 0;

      if (currentOffset < partition.messages.length) {
        const message = partition.messages[currentOffset];
        consumer.currentOffsets.set(pid, currentOffset + 1);
        return message;
      }
    }

    return null; // no new messages
  }

  /** Commit current offsets for a consumer (snapshot current → committed). */
  commitOffsets(consumerId: string, groupId: string): void {
    const group = this.consumerGroups.find((g) => g.id === groupId);
    if (!group) return;

    const consumer = group.consumers.find((c) => c.id === consumerId);
    if (!consumer) return;

    for (const pid of consumer.assignedPartitions) {
      const current = consumer.currentOffsets.get(pid);
      if (current !== undefined) {
        consumer.committedOffsets.set(pid, current);
      }
    }
  }

  // -----------------------------------------------------------
  // 5.  State Access
  // -----------------------------------------------------------

  /** Return a deep-cloned snapshot of the full Kafka state. */
  getState(): KafkaState {
    return {
      brokers: deepCloneBrokers(this.brokers),
      partitions: deepClonePartitions(this.partitions),
      consumerGroups: deepCloneConsumerGroups(this.consumerGroups),
      producerConfig: { ...this.producerConfig },
      clusterConfig: { ...this.clusterConfig },
      totalMessagesProduced: this.totalMessagesProduced,
      isRunning: this.isRunning,
    };
  }

  // -----------------------------------------------------------
  // 6.  Reset
  // -----------------------------------------------------------

  /** Reset the engine back to its initial (empty) state. */
  reset(): void {
    this.initCluster(this.clusterConfig);
  }

  // -----------------------------------------------------------
  //     Setters / Utilities
  // -----------------------------------------------------------

  setProducerConfig(config: Partial<ProducerConfig>): void {
    this.producerConfig = { ...this.producerConfig, ...config };
  }

  setRunning(running: boolean): void {
    this.isRunning = running;
  }
}
