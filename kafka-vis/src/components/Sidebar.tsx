import { useState } from 'react';
import type { KafkaState, ClusterConfig } from '../types';

interface SidebarProps {
  state: KafkaState;
  isRunning: boolean;
  produceInterval: number;
  consumeInterval: number;
  commitInterval: number;
  acks: 'all' | '1' | '0';
  onToggleSimulation: () => void;
  onProduceIntervalChange: (val: number) => void;
  onConsumeIntervalChange: (val: number) => void;
  onCommitIntervalChange: (val: number) => void;
  onAcksChange: (val: 'all' | '1' | '0') => void;
  onUpdateConfig: (config: Partial<ClusterConfig>) => void;
  onToggleBroker: (brokerId: number) => void;
  onAddConsumer: (groupId: string) => void;
  onRemoveConsumer: (groupId: string, consumerId: string) => void;
  onReset: () => void;
  onProduceOne: () => void;
  onCommitNow: () => void;
  animDuration: number;
  onAnimDurationChange: (val: number) => void;
}

export function Sidebar({
  state,
  isRunning,
  produceInterval,
  consumeInterval,
  commitInterval,
  acks,
  onToggleSimulation,
  onProduceIntervalChange,
  onConsumeIntervalChange,
  onCommitIntervalChange,
  onAcksChange,
  onUpdateConfig,
  onToggleBroker,
  onAddConsumer,
  onRemoveConsumer,
  onReset,
  onProduceOne,
  onCommitNow,
  animDuration,
  onAnimDurationChange,
}: SidebarProps) {
  const [newGroupId, setNewGroupId] = useState('group-1');
  const { clusterConfig } = state;

  return (
    <aside className="sidebar">
      {/* Simulation Controls */}
      <div className="panel">
        <div className="panel-header">
          <h3>⚡ Simulation</h3>
        </div>
        <div className="panel-content">
          <div className="control-buttons">
            <button
              className={`btn ${isRunning ? 'btn-danger' : 'btn-success'} btn-block`}
              onClick={onToggleSimulation}
            >
              {isRunning ? '⏸ Pause' : '▶ Start'}
            </button>
            <div className="btn-row">
              <button className="btn btn-primary btn-sm" onClick={onProduceOne} title="Produce a message immediately">
                + Produce
              </button>
              <button className="btn btn-success btn-sm" onClick={onCommitNow} title="Commit uncommitted offsets immediately">
                ✓ Commit
              </button>
              <button className="btn btn-outline btn-sm" onClick={onReset} title="Reset cluster state">
                ↺ Reset
              </button>
            </div>
          </div>

          <div className="slider-control" style={{ marginTop: '6px' }}>
            <label>
              <span>Animation Speed (Duration)</span>
              <span className="slider-value">{(animDuration / 1000).toFixed(1)}s</span>
            </label>
            <input
              type="range"
              min="500"
              max="2500"
              step="100"
              value={animDuration}
              onChange={(e) => onAnimDurationChange(Number(e.target.value))}
            />
          </div>
        </div>
      </div>

      {/* Cluster Configuration */}
      <div className="panel">
        <div className="panel-header">
          <h3>🏗️ Cluster Configuration</h3>
        </div>
        <div className="panel-content">
          <div className="slider-control">
            <label>
              <span>Brokers</span>
              <span className="slider-value">{clusterConfig.numBrokers}</span>
            </label>
            <input
              type="range"
              min="1"
              max="5"
              value={clusterConfig.numBrokers}
              onChange={(e) => onUpdateConfig({ numBrokers: Number(e.target.value) })}
              disabled={isRunning}
            />
          </div>

          <div className="slider-control">
            <label>
              <span>Partitions</span>
              <span className="slider-value">{clusterConfig.numPartitions}</span>
            </label>
            <input
              type="range"
              min="1"
              max="5"
              value={clusterConfig.numPartitions}
              onChange={(e) => onUpdateConfig({ numPartitions: Number(e.target.value) })}
              disabled={isRunning}
            />
          </div>

          <div className="slider-control">
            <label>
              <span>Replication Factor</span>
              <span className="slider-value">{clusterConfig.replicationFactor}</span>
            </label>
            <input
              type="range"
              min="1"
              max={clusterConfig.numBrokers}
              value={Math.min(clusterConfig.replicationFactor, clusterConfig.numBrokers)}
              onChange={(e) => onUpdateConfig({ replicationFactor: Number(e.target.value) })}
              disabled={isRunning}
            />
          </div>

          <div className="slider-control">
            <label>
              <span>Min ISR</span>
              <span className="slider-value">{clusterConfig.minInsyncReplicas}</span>
            </label>
            <input
              type="range"
              min="1"
              max={clusterConfig.replicationFactor}
              value={Math.min(clusterConfig.minInsyncReplicas, clusterConfig.replicationFactor)}
              onChange={(e) => onUpdateConfig({ minInsyncReplicas: Number(e.target.value) })}
              disabled={isRunning}
            />
          </div>
        </div>
      </div>

      {/* Producer Configuration */}
      <div className="panel">
        <div className="panel-header">
          <h3>📤 Producer</h3>
        </div>
        <div className="panel-content">
          <div className="slider-control">
            <label>
              <span>Produce Interval</span>
              <span className="slider-value">{(produceInterval / 1000).toFixed(1)}s</span>
            </label>
            <input
              type="range"
              min="200"
              max="5000"
              step="100"
              value={produceInterval}
              onChange={(e) => onProduceIntervalChange(Number(e.target.value))}
            />
          </div>

          <div className="select-control">
            <label>Acks</label>
            <select value={acks} onChange={(e) => onAcksChange(e.target.value as 'all' | '1' | '0')}>
              <option value="all">all (strongest)</option>
              <option value="1">1 (leader only)</option>
              <option value="0">0 (fire & forget)</option>
            </select>
          </div>
        </div>
      </div>

      {/* Consumer Configuration */}
      <div className="panel">
        <div className="panel-header">
          <h3>📥 Consumers</h3>
        </div>
        <div className="panel-content">
          <div className="slider-control">
            <label>
              <span>Consume Interval</span>
              <span className="slider-value">{(consumeInterval / 1000).toFixed(1)}s</span>
            </label>
            <input
              type="range"
              min="500"
              max="10000"
              step="100"
              value={consumeInterval}
              onChange={(e) => onConsumeIntervalChange(Number(e.target.value))}
            />
          </div>

          <div className="slider-control">
            <label>
              <span>Commit Interval</span>
              <span className="slider-value">{(commitInterval / 1000).toFixed(1)}s</span>
            </label>
            <input
              type="range"
              min="1000"
              max="15000"
              step="500"
              value={commitInterval}
              onChange={(e) => onCommitIntervalChange(Number(e.target.value))}
            />
          </div>

          <div className="consumer-group-controls">
            <div className="add-consumer-row">
              <input
                type="text"
                value={newGroupId}
                onChange={(e) => setNewGroupId(e.target.value)}
                placeholder="Group ID"
                className="input-field"
              />
              <button
                className="btn btn-primary btn-sm"
                onClick={() => onAddConsumer(newGroupId)}
              >
                + Add
              </button>
            </div>
          </div>

          {state.consumerGroups.map(group => (
            <div key={group.id} className="consumer-group-item">
              <div className="consumer-group-header">
                <span className="consumer-group-dot" style={{ backgroundColor: group.color }} />
                <span className="consumer-group-name">{group.id}</span>
                <span className="consumer-count">{group.consumers.length} consumer(s)</span>
              </div>
              {group.consumers.map(consumer => (
                <div key={consumer.id} className="consumer-entry">
                  <span className="consumer-id">{consumer.id}</span>
                  <span className="consumer-partitions">
                    P[{consumer.assignedPartitions.join(', ')}]
                  </span>
                  <button
                    className="btn btn-danger btn-xs"
                    onClick={() => onRemoveConsumer(group.id, consumer.id)}
                  >
                    ✕
                  </button>
                </div>
              ))}
              <button
                className="btn btn-outline btn-sm btn-block"
                onClick={() => onAddConsumer(group.id)}
              >
                + Add Consumer
              </button>
            </div>
          ))}
        </div>
      </div>

      {/* Broker Controls */}
      <div className="panel">
        <div className="panel-header">
          <h3>🖥️ Brokers</h3>
        </div>
        <div className="panel-content">
          <div className="broker-toggle-list">
            {state.brokers.map(broker => (
              <div key={broker.id} className="broker-toggle-item">
                <span className={`broker-status-indicator ${broker.isAlive ? 'alive' : 'dead'}`} />
                <span className="broker-name">Broker {broker.id}</span>
                <button
                  className={`btn btn-sm ${broker.isAlive ? 'btn-danger' : 'btn-success'}`}
                  onClick={() => onToggleBroker(broker.id)}
                >
                  {broker.isAlive ? 'Stop' : 'Start'}
                </button>
              </div>
            ))}
          </div>
        </div>
      </div>
    </aside>
  );
}
