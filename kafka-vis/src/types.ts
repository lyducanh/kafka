// ============================================================
// Kafka Visualization - Type Definitions
// ============================================================

export interface Message {
  id: number;
  key: string;
  value: string;
  partition: number;
  offset: number;
  timestamp: number;
}

export interface Partition {
  id: number;
  messages: Message[];
  leaderId: number;       // broker ID that is the leader
  replicaIds: number[];   // broker IDs holding replicas
  isrIds: number[];       // in-sync replica broker IDs
  nextOffset: number;
}

export interface Broker {
  id: number;
  isAlive: boolean;
  partitionLeaders: number[];   // partition IDs this broker leads
  partitionReplicas: number[];  // partition IDs this broker holds replicas for
}

export interface Consumer {
  id: string;
  groupId: string;
  assignedPartitions: number[];
  committedOffsets: Map<number, number>;  // partition -> committed offset
  currentOffsets: Map<number, number>;    // partition -> current read position
  isActive: boolean;
  consumeInterval: number;
  commitInterval: number;
  color: string;
}

export interface ConsumerGroup {
  id: string;
  consumers: Consumer[];
  color: string;
}

export interface ProducerConfig {
  produceInterval: number;  // ms between producing messages
  acks: 'all' | '1' | '0';
}

export interface ClusterConfig {
  numBrokers: number;
  numPartitions: number;
  replicationFactor: number;
  minInsyncReplicas: number;
}

export interface KafkaState {
  brokers: Broker[];
  partitions: Partition[];
  consumerGroups: ConsumerGroup[];
  producerConfig: ProducerConfig;
  clusterConfig: ClusterConfig;
  totalMessagesProduced: number;
  isRunning: boolean;
}

export interface AnimatedMessage {
  id: string;
  fromX: number;
  fromY: number;
  toX: number;
  toY: number;
  startTime: number;
  duration: number;
  type: 'produce' | 'consume' | 'replicate';
  color: string;
  label?: string;
}
