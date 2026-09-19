import { useState, useCallback, useEffect, useRef } from 'react';
import { KafkaEngine, CONSUMER_COLORS } from './engine';
import type { KafkaState, ClusterConfig, Message } from './types';
import { Sidebar } from './components/Sidebar';
import { Visualization } from './components/Visualization';
import { InfoPanel } from './components/InfoPanel';
import { AnimationLayer, type FlyingMessage } from './components/AnimationLayer';

const DEFAULT_CONFIG: ClusterConfig = {
  numBrokers: 3,
  numPartitions: 3,
  replicationFactor: 2,
  minInsyncReplicas: 1,
};

function initDefaultEngine(): KafkaEngine {
  const engine = new KafkaEngine(DEFAULT_CONFIG);
  engine.addConsumer('group-1');
  engine.addConsumer('group-1');
  return engine;
}

function App() {
  const engineRef = useRef<KafkaEngine>(initDefaultEngine());
  const [state, setState] = useState<KafkaState>(engineRef.current.getState());
  const [isRunning, setIsRunning] = useState(false);
  const [produceInterval, setProduceInterval] = useState(2000);
  const [consumeInterval, setConsumeInterval] = useState(3000);
  const [commitInterval, setCommitInterval] = useState(5000);
  const [acks, setAcks] = useState<'all' | '1' | '0'>('all');
  const [animDuration, setAnimDuration] = useState(1300);
  const [flyingMessages, setFlyingMessages] = useState<FlyingMessage[]>([]);
  const [recentProduced, setRecentProduced] = useState<Message[]>([]);
  const [messageLog, setMessageLog] = useState<string[]>([]);

  const produceTimerRef = useRef<number | null>(null);
  const consumeTimerRef = useRef<number | null>(null);
  const commitTimerRef = useRef<number | null>(null);
  const messageCounterRef = useRef(0);
  const animIdRef = useRef(0);
  const vizContainerRef = useRef<HTMLDivElement>(null);

  // Get element center position relative to viz-container
  const getElementCenter = useCallback((selector: string, fallbackSelector?: string) => {
    const container = vizContainerRef.current;
    if (!container) return null;
    let el = container.querySelector(selector);
    if (!el && fallbackSelector) {
      el = container.querySelector(fallbackSelector);
    }
    if (!el) return null;
    const containerRect = container.getBoundingClientRect();
    const elRect = el.getBoundingClientRect();
    return {
      x: elRect.left + elRect.width / 2 - containerRect.left,
      y: elRect.top + elRect.height / 2 - containerRect.top,
    };
  }, []);

  const spawnAnimation = useCallback((
    fromSelector: string,
    toSelector: string,
    color: string,
    type: FlyingMessage['type'],
    label?: string,
    badgeText?: string,
    durationOverride?: number,
    fromFallback?: string,
    toFallback?: string,
  ) => {
    const from = getElementCenter(fromSelector, fromFallback);
    const to = getElementCenter(toSelector, toFallback);
    if (!from || !to) return;

    const id = `anim-${++animIdRef.current}`;
    const anim: FlyingMessage = {
      id,
      fromX: from.x,
      fromY: from.y,
      toX: to.x,
      toY: to.y,
      color,
      duration: durationOverride ?? animDuration,
      type,
      label,
      badgeText,
      startTime: Date.now(),
    };
    setFlyingMessages(prev => [...prev, anim]);
  }, [getElementCenter, animDuration]);

  const removeAnimation = useCallback((id: string) => {
    setFlyingMessages(prev => prev.filter(a => a.id !== id));
  }, []);

  const addLog = useCallback((msg: string) => {
    setMessageLog(prev => {
      const next = [`[${new Date().toLocaleTimeString()}] ${msg}`, ...prev];
      return next.slice(0, 100);
    });
  }, []);

  const refreshState = useCallback(() => {
    setState(engineRef.current.getState());
  }, []);

  // Update cluster config
  const updateConfig = useCallback((newConfig: Partial<ClusterConfig>) => {
    const engine = engineRef.current;
    const currentConfig = engine.getState().clusterConfig;
    const merged = { ...currentConfig, ...newConfig };

    // Validate
    if (merged.replicationFactor > merged.numBrokers) {
      merged.replicationFactor = merged.numBrokers;
    }
    if (merged.minInsyncReplicas > merged.replicationFactor) {
      merged.minInsyncReplicas = merged.replicationFactor;
    }

    // Re-create engine with new config, preserving consumer groups
    const oldState = engine.getState();
    const newEngine = new KafkaEngine(merged);
    
    // Re-add consumer groups
    oldState.consumerGroups.forEach(group => {
      group.consumers.forEach(() => {
        newEngine.addConsumer(group.id);
      });
    });

    engineRef.current = newEngine;
    refreshState();
    addLog(`Cluster reconfigured: ${merged.numBrokers} brokers, ${merged.numPartitions} partitions, RF=${merged.replicationFactor}`);
  }, [refreshState, addLog]);

  // Producer
  const produceMessage = useCallback(() => {
    const engine = engineRef.current;
    messageCounterRef.current++;
    const key = `key-${Math.floor(Math.random() * 100)}`;
    const value = `msg-${messageCounterRef.current}`;
    const result = engine.produce(key, value);
    if (result) {
      setRecentProduced(prev => [result, ...prev].slice(0, 6));
      addLog(`Produced: ${value} → partition ${result.partition} (offset ${result.offset})`);
      spawnAnimation(
        '[data-producer="true"]',
        `[data-partition-bar="${result.partition}"]`,
        '#2196F3',
        'produce',
        `📤 ${value} (#${result.offset})`,
        `#${result.offset}`,
      );
    } else {
      addLog(`⚠ Failed to produce: ${value} (no available partition)`);
    }
    refreshState();
  }, [refreshState, addLog, spawnAnimation]);

  // Consumer
  const consumeMessages = useCallback(() => {
    const engine = engineRef.current;
    const state = engine.getState();
    state.consumerGroups.forEach(group => {
      group.consumers.forEach(consumer => {
        if (consumer.isActive) {
          const result = engine.consume(consumer.id, group.id);
          if (result) {
            addLog(`Consumer ${consumer.id} (${group.id}): consumed from P${result.partition} offset ${result.offset}`);
            spawnAnimation(
              `[data-partition-bar="${result.partition}"]`,
              `[data-consumer-partition="${consumer.id}-${result.partition}"]`,
              consumer.color,
              'consume',
              `📥 ${consumer.id} (#${result.offset})`,
              `#${result.offset}`,
              undefined,
              undefined,
              `[data-consumer="${consumer.id}"]`,
            );
          }
        }
      });
    });
    refreshState();
  }, [refreshState, addLog, spawnAnimation]);

  // Commit
  const commitOffsets = useCallback(() => {
    const engine = engineRef.current;
    const state = engine.getState();
    let committedAny = false;

    state.consumerGroups.forEach(group => {
      group.consumers.forEach(consumer => {
        if (consumer.isActive) {
          consumer.assignedPartitions.forEach(pId => {
            const current = consumer.currentOffsets.get(pId) ?? 0;
            const committed = consumer.committedOffsets.get(pId) ?? 0;
            if (current > committed) {
              committedAny = true;
              addLog(`✓ Consumer ${consumer.id} (${group.id}): committed offset ${current} for P${pId}`);
              // Spawn commit animation from consumer partition container back to broker partition bar
              spawnAnimation(
                `[data-consumer-partition="${consumer.id}-${pId}"]`,
                `[data-partition-bar="${pId}"]`,
                '#4CAF50',
                'commit',
                `✓ Commit P${pId} (#${current - 1})`,
                '✓',
                undefined,
                `[data-consumer="${consumer.id}"]`,
              );
            }
          });
          engine.commitOffsets(consumer.id, group.id);
        }
      });
    });

    if (!committedAny) {
      addLog('Offsets checked: all current offsets already committed');
    }
    refreshState();
  }, [refreshState, addLog, spawnAnimation]);

  // Start/Stop simulation
  const toggleSimulation = useCallback(() => {
    if (isRunning) {
      // Stop
      if (produceTimerRef.current) clearInterval(produceTimerRef.current);
      if (consumeTimerRef.current) clearInterval(consumeTimerRef.current);
      if (commitTimerRef.current) clearInterval(commitTimerRef.current);
      produceTimerRef.current = null;
      consumeTimerRef.current = null;
      commitTimerRef.current = null;
      setIsRunning(false);
      addLog('⏸ Simulation paused');
    } else {
      // Start
      produceTimerRef.current = window.setInterval(produceMessage, produceInterval);
      consumeTimerRef.current = window.setInterval(consumeMessages, consumeInterval);
      commitTimerRef.current = window.setInterval(commitOffsets, commitInterval);
      setIsRunning(true);
      addLog('▶ Simulation started');
    }
  }, [isRunning, produceInterval, consumeInterval, commitInterval, produceMessage, consumeMessages, commitOffsets, addLog]);

  // Update intervals when they change
  useEffect(() => {
    if (isRunning) {
      if (produceTimerRef.current) clearInterval(produceTimerRef.current);
      produceTimerRef.current = window.setInterval(produceMessage, produceInterval);
    }
  }, [produceInterval, isRunning, produceMessage]);

  useEffect(() => {
    if (isRunning) {
      if (consumeTimerRef.current) clearInterval(consumeTimerRef.current);
      consumeTimerRef.current = window.setInterval(consumeMessages, consumeInterval);
    }
  }, [consumeInterval, isRunning, consumeMessages]);

  useEffect(() => {
    if (isRunning) {
      if (commitTimerRef.current) clearInterval(commitTimerRef.current);
      commitTimerRef.current = window.setInterval(commitOffsets, commitInterval);
    }
  }, [commitInterval, isRunning, commitOffsets]);

  // Cleanup on unmount
  useEffect(() => {
    return () => {
      if (produceTimerRef.current) clearInterval(produceTimerRef.current);
      if (consumeTimerRef.current) clearInterval(consumeTimerRef.current);
      if (commitTimerRef.current) clearInterval(commitTimerRef.current);
    };
  }, []);

  // Broker toggle
  const toggleBroker = useCallback((brokerId: number) => {
    const engine = engineRef.current;
    const broker = engine.getState().brokers.find(b => b.id === brokerId);
    engine.toggleBroker(brokerId);
    refreshState();
    addLog(`Broker ${brokerId} ${broker?.isAlive ? 'stopped' : 'started'}`);
  }, [refreshState, addLog]);

  // Consumer management
  const addConsumer = useCallback((groupId: string) => {
    const engine = engineRef.current;
    const consumer = engine.addConsumer(groupId);
    refreshState();
    addLog(`Added ${consumer.id} to group "${groupId}"`);
  }, [refreshState, addLog]);

  const removeConsumer = useCallback((groupId: string, consumerId: string) => {
    const engine = engineRef.current;
    engine.removeConsumer(groupId, consumerId);
    refreshState();
    addLog(`Removed ${consumerId} from group "${groupId}"`);
  }, [refreshState, addLog]);

  // Reset
  const resetAll = useCallback(() => {
    if (isRunning) {
      if (produceTimerRef.current) clearInterval(produceTimerRef.current);
      if (consumeTimerRef.current) clearInterval(consumeTimerRef.current);
      if (commitTimerRef.current) clearInterval(commitTimerRef.current);
      produceTimerRef.current = null;
      consumeTimerRef.current = null;
      commitTimerRef.current = null;
      setIsRunning(false);
    }
    engineRef.current = initDefaultEngine();
    messageCounterRef.current = 0;
    setRecentProduced([]);
    refreshState();
    setMessageLog([]);
    addLog('🔄 Reset complete (default consumers restored)');
  }, [isRunning, refreshState, addLog]);

  // Update acks
  useEffect(() => {
    engineRef.current.setAcks(acks);
  }, [acks]);

  return (
    <div className="app">
      <header className="app-header">
        <div className="header-content">
          <div className="header-logo">
            <svg width="32" height="32" viewBox="0 0 32 32" fill="none">
              <circle cx="16" cy="16" r="14" stroke="#fff" strokeWidth="2"/>
              <circle cx="16" cy="10" r="3" fill="#fff"/>
              <circle cx="10" cy="20" r="3" fill="#fff"/>
              <circle cx="22" cy="20" r="3" fill="#fff"/>
              <line x1="16" y1="13" x2="10" y2="17" stroke="#fff" strokeWidth="1.5"/>
              <line x1="16" y1="13" x2="22" y2="17" stroke="#fff" strokeWidth="1.5"/>
              <line x1="10" y1="20" x2="22" y2="20" stroke="#fff" strokeWidth="1.5"/>
            </svg>
            <h1>Kafka Visualization</h1>
          </div>
          <p className="header-subtitle">
            Interactive simulation of Apache Kafka's message processing model
          </p>
        </div>
      </header>

      <div className="main-content">
        <Sidebar
          state={state}
          isRunning={isRunning}
          produceInterval={produceInterval}
          consumeInterval={consumeInterval}
          commitInterval={commitInterval}
          acks={acks}
          onToggleSimulation={toggleSimulation}
          onProduceIntervalChange={setProduceInterval}
          onConsumeIntervalChange={setConsumeInterval}
          onCommitIntervalChange={setCommitInterval}
          onAcksChange={setAcks}
          onUpdateConfig={updateConfig}
          onToggleBroker={toggleBroker}
          onAddConsumer={addConsumer}
          onRemoveConsumer={removeConsumer}
          onReset={resetAll}
          onProduceOne={produceMessage}
          onCommitNow={commitOffsets}
          animDuration={animDuration}
          onAnimDurationChange={setAnimDuration}
        />
        <div className="viz-container" ref={vizContainerRef}>
          <AnimationLayer animations={flyingMessages} onAnimationEnd={removeAnimation} />
          <Visualization state={state} recentProduced={recentProduced} />
          <InfoPanel messageLog={messageLog} />
        </div>
      </div>
    </div>
  );
}

export default App;
