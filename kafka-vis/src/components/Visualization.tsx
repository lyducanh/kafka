import type { KafkaState, Message } from '../types';

interface VisualizationProps {
  state: KafkaState;
  recentProduced?: Message[];
}

export function Visualization({ state, recentProduced = [] }: VisualizationProps) {
  const { brokers, partitions, consumerGroups } = state;

  // Find which consumer group/consumer is assigned to each partition
  const partitionConsumerMap = new Map<number, { consumerId: string; groupId: string; color: string }>();
  consumerGroups.forEach(group => {
    group.consumers.forEach(consumer => {
      consumer.assignedPartitions.forEach(pId => {
        partitionConsumerMap.set(pId, {
          consumerId: consumer.id,
          groupId: group.id,
          color: consumer.color,
        });
      });
    });
  });

  return (
    <div className="viz-area">
      {/* Producer Section */}
      <div className="viz-section">
        <div className="viz-section-label">📤 Message Producer</div>
        <div className="producer-container" data-producer="true">
          <div className="producer-box">
            <div className="producer-icon">
              <svg width="36" height="36" viewBox="0 0 40 40" fill="none">
                <rect x="4" y="8" width="32" height="24" rx="6" fill="#2196F3" opacity="0.15" stroke="#2196F3" strokeWidth="2.5"/>
                <path d="M20 14 L20 26 M14 20 L26 20" stroke="#2196F3" strokeWidth="2.5" strokeLinecap="round"/>
              </svg>
            </div>
            <div className="producer-stats">
              <span className="stat-label">Messages Produced</span>
              <span className="stat-value">{state.totalMessagesProduced}</span>
            </div>
            <div className="producer-config-display">
              <span className="badge badge-blue">acks={state.producerConfig.acks}</span>
            </div>
          </div>

          {/* Producer Outbox Stream */}
          <div className="producer-outbox">
            <div className="producer-outbox-title">
              <span>Producer Outbox (Recent Sent)</span>
              <span className="producer-outbox-count">
                {recentProduced.length > 0 ? `${recentProduced.length} recent` : 'Idle'}
              </span>
            </div>
            <div className="producer-msg-track">
              {recentProduced.length === 0 ? (
                <div className="producer-empty-hint">
                  Waiting to produce... Hit <strong>+ Produce</strong> or <strong>▶ Start</strong>
                </div>
              ) : (
                recentProduced.map((msg, idx) => (
                  <div
                    key={msg.id}
                    className={`producer-msg-card ${idx === 0 ? 'is-latest' : ''}`}
                    title={`Key: ${msg.key}\nValue: ${msg.value}\nTarget: Partition ${msg.partition}\nOffset: #${msg.offset}`}
                  >
                    <div className="p-msg-top">
                      <span className="p-msg-offset">#{msg.offset}</span>
                      <span className="p-msg-dest">→ P{msg.partition}</span>
                    </div>
                    <span className="p-msg-val">{msg.value}</span>
                  </div>
                ))
              )}
            </div>
          </div>
        </div>
      </div>

      {/* Flow arrow */}
      <div className="viz-arrow">
        <svg width="24" height="36" viewBox="0 0 24 36">
          <path d="M12 0 L12 26 M6 20 L12 28 L18 20" stroke="#64748b" strokeWidth="2.5" fill="none" strokeLinecap="round" strokeLinejoin="round"/>
        </svg>
      </div>

      {/* Kafka Cluster (Brokers & Partitions) */}
      <div className="viz-section">
        <div className="viz-section-label">🏢 Kafka Cluster (Brokers & Topic Partitions)</div>
        <div className="brokers-container">
          {brokers.map(broker => {
            const brokerPartitions = partitions.filter(
              p => p.leaderId === broker.id || p.replicaIds.includes(broker.id)
            );
            return (
              <div
                key={broker.id}
                className={`broker-card ${!broker.isAlive ? 'offline' : ''}`}
              >
                <div className="broker-header">
                  <span className={`broker-status-dot ${broker.isAlive ? 'alive' : 'dead'}`} />
                  <span className="broker-title">Broker {broker.id}</span>
                  {!broker.isAlive ? (
                    <span className="badge badge-red">OFFLINE</span>
                  ) : (
                    <span className="badge badge-green">ONLINE</span>
                  )}
                </div>
                <div className="broker-partitions">
                  {brokerPartitions.length === 0 ? (
                    <div className="no-partitions">No assigned partitions</div>
                  ) : (
                    brokerPartitions.map(partition => {
                      const isLeader = partition.leaderId === broker.id;
                      const isIsr = partition.isrIds.includes(broker.id);
                      const maxMessages = 12;
                      const visibleMessages = partition.messages.slice(-maxMessages);

                      return (
                        <div key={partition.id} className="partition-row">
                          <div className="partition-info">
                            <span className="partition-label">Partition {partition.id}</span>
                            {isLeader && <span className="badge badge-gold">★ Leader</span>}
                            {!isLeader && isIsr && <span className="badge badge-green">ISR</span>}
                            {!isLeader && !isIsr && <span className="badge badge-gray">Replica</span>}
                            <span className="partition-offset-tag">Log End: #{partition.nextOffset}</span>
                          </div>
                          <div
                            className="partition-bar"
                            data-partition-bar={isLeader ? partition.id : undefined}
                          >
                            {visibleMessages.length === 0 ? (
                              <span className="partition-empty-text">Empty partition</span>
                            ) : (
                              visibleMessages.map(msg => {
                                let consumed = false;
                                let consumedColor = '#2196F3';
                                consumerGroups.forEach(group => {
                                  group.consumers.forEach(c => {
                                    const currentOff = c.currentOffsets.get(partition.id);
                                    if (currentOff !== undefined && msg.offset < currentOff) {
                                      consumed = true;
                                      consumedColor = c.color;
                                    }
                                  });
                                });

                                let committedByAny = false;
                                consumerGroups.forEach(group => {
                                  group.consumers.forEach(c => {
                                    const committedOff = c.committedOffsets.get(partition.id);
                                    if (committedOff !== undefined && msg.offset < committedOff) {
                                      committedByAny = true;
                                    }
                                  });
                                });

                                return (
                                  <div
                                    key={msg.id}
                                    className={`message-dot ${consumed ? 'consumed' : ''} ${committedByAny ? 'committed' : ''}`}
                                    style={{
                                      backgroundColor: consumed ? consumedColor : '#2196F3',
                                    }}
                                    title={`Key: ${msg.key}\nValue: ${msg.value}\nOffset: #${msg.offset}\n${consumed ? '✓ Consumed by subscriber' : '○ Not yet read'}${committedByAny ? '\n✓ Offset Committed' : ''}`}
                                  >
                                    <span className="msg-num">#{msg.offset}</span>
                                    {committedByAny && <span className="msg-check">✓</span>}
                                  </div>
                                );
                              })
                            )}
                            {partition.messages.length > maxMessages && (
                              <span className="more-messages">+{partition.messages.length - maxMessages}</span>
                            )}
                          </div>
                        </div>
                      );
                    })
                  )}
                </div>
              </div>
            );
          })}
        </div>
      </div>

      {/* Flow arrow */}
      {consumerGroups.length > 0 && (
        <div className="viz-arrow">
          <svg width="24" height="36" viewBox="0 0 24 36">
            <path d="M12 0 L12 26 M6 20 L12 28 L18 20" stroke="#64748b" strokeWidth="2.5" fill="none" strokeLinecap="round" strokeLinejoin="round"/>
          </svg>
        </div>
      )}

      {/* Consumer Groups Section */}
      {consumerGroups.length > 0 && (
        <div className="viz-section">
          <div className="viz-section-label">📥 Consumer Groups (Subscribers)</div>
          <div className="consumer-groups-container">
            {consumerGroups.map(group => (
              <div key={group.id} className="consumer-group-card">
                <div className="cg-header">
                  <span className="cg-dot" style={{ backgroundColor: group.color }} />
                  <span className="cg-title">{group.id}</span>
                  <span className="cg-count-badge">{group.consumers.length} Consumers</span>
                </div>
                <div className="cg-consumers">
                  {group.consumers.map(consumer => (
                    <div key={consumer.id} className="cg-consumer" data-consumer={consumer.id}>
                      <div className="cg-consumer-header">
                        <span className="cg-consumer-dot" style={{ backgroundColor: consumer.color }} />
                        <span className="cg-consumer-name">{consumer.id}</span>
                        <span className="cg-assigned-count">
                          {consumer.assignedPartitions.length > 0
                            ? `Partitions: [${consumer.assignedPartitions.map(p => `P${p}`).join(', ')}]`
                            : 'No Partitions'}
                        </span>
                      </div>

                      <div className="cg-consumer-detail">
                        {consumer.assignedPartitions.length === 0 ? (
                          <div className="cg-idle-box">
                            <span>⏸ Idle (no partitions assigned to this consumer)</span>
                          </div>
                        ) : (
                          consumer.assignedPartitions.map(pId => {
                            const partition = partitions.find(p => p.id === pId);
                            if (!partition) return null;
                            const currentOff = consumer.currentOffsets.get(pId) ?? 0;
                            const committedOff = consumer.committedOffsets.get(pId) ?? 0;
                            const consumedMsgs = partition.messages.filter(m => m.offset < currentOff);

                            return (
                              <div
                                key={pId}
                                className="consumer-partition-box"
                                data-consumer-partition={`${consumer.id}-${pId}`}
                              >
                                <div className="cp-header">
                                  <div className="cp-title-wrap">
                                    <span className="cp-partition-badge">Partition {pId}</span>
                                    <span className="cp-offset-stat">
                                      Read: <strong>#{currentOff}</strong> | Committed: <strong>#{committedOff}</strong> / #{partition.nextOffset}
                                    </span>
                                  </div>
                                  {currentOff > committedOff ? (
                                    <span className="cp-status-badge uncommitted">
                                      ⏳ {currentOff - committedOff} Uncommitted
                                    </span>
                                  ) : (
                                    currentOff > 0 && (
                                      <span className="cp-status-badge committed">
                                        ✓ All Committed
                                      </span>
                                    )
                                  )}
                                </div>

                                {/* Visual Partition Container on Consumer */}
                                <div className="consumer-partition-track">
                                  {consumedMsgs.length === 0 ? (
                                    <div className="cp-track-empty">
                                      <span>Waiting for messages on Partition {pId}...</span>
                                    </div>
                                  ) : (
                                    consumedMsgs.map(m => {
                                      const isCommitted = m.offset < committedOff;
                                      return (
                                        <div
                                          key={m.id}
                                          className={`consumer-msg-card ${isCommitted ? 'is-committed' : 'is-uncommitted'}`}
                                          title={`Value: ${m.value}\nKey: ${m.key}\nOffset: #${m.offset}\nStatus: ${isCommitted ? 'Committed' : 'Read (Uncommitted)'}`}
                                        >
                                          <div className="cm-top">
                                            <span className="cm-num">#{m.offset}</span>
                                            <span className="cm-status-icon">{isCommitted ? '✓' : '⏳'}</span>
                                          </div>
                                          <span className="cm-val">{m.value}</span>
                                        </div>
                                      );
                                    })
                                  )}
                                </div>
                              </div>
                            );
                          })
                        )}
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}
