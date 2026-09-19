import { useState } from 'react';

interface InfoPanelProps {
  messageLog: string[];
}

export function InfoPanel({ messageLog }: InfoPanelProps) {
  const [activeTab, setActiveTab] = useState<'log' | 'notes'>('log');
  const [isExpanded, setIsExpanded] = useState(true);

  return (
    <div className={`info-panel ${isExpanded ? 'expanded' : 'collapsed'}`}>
      <div className="info-panel-header" onClick={() => setIsExpanded(!isExpanded)}>
        <div className="info-tabs">
          <button
            className={`info-tab ${activeTab === 'log' ? 'active' : ''}`}
            onClick={(e) => { e.stopPropagation(); setActiveTab('log'); setIsExpanded(true); }}
          >
            📋 Event Log
          </button>
          <button
            className={`info-tab ${activeTab === 'notes' ? 'active' : ''}`}
            onClick={(e) => { e.stopPropagation(); setActiveTab('notes'); setIsExpanded(true); }}
          >
            📝 Notes
          </button>
        </div>
        <button className="info-toggle">{isExpanded ? '▼' : '▲'}</button>
      </div>

      {isExpanded && (
        <div className="info-panel-content">
          {activeTab === 'log' ? (
            <div className="message-log">
              {messageLog.length === 0 ? (
                <p className="log-empty">No events yet. Start the simulation or produce a message.</p>
              ) : (
                messageLog.map((msg, i) => (
                  <div key={i} className="log-entry">{msg}</div>
                ))
              )}
            </div>
          ) : (
            <div className="notes-content">
              <h4>How Kafka Works</h4>
              <div className="note-section">
                <h5>📦 Partitions & Replication</h5>
                <p>
                  Messages are distributed across <strong>partitions</strong> based on the message key. 
                  Each partition is replicated across multiple brokers for fault tolerance. One replica is 
                  the <strong>leader</strong> (handles reads/writes), others are <strong>followers</strong>.
                </p>
              </div>
              <div className="note-section">
                <h5>🔄 ISR (In-Sync Replicas)</h5>
                <p>
                  The ISR set contains replicas that are fully caught up with the leader. When a broker 
                  goes down, it's removed from the ISR. The <code>min.insync.replicas</code> setting 
                  determines the minimum ISR size needed for writes when <code>acks=all</code>.
                </p>
              </div>
              <div className="note-section">
                <h5>📤 Producer Acks</h5>
                <ul>
                  <li><code>acks=0</code>: Fire and forget. No acknowledgment.</li>
                  <li><code>acks=1</code>: Leader acknowledges. Fast but may lose data.</li>
                  <li><code>acks=all</code>: All ISR replicas must acknowledge. Strongest durability.</li>
                </ul>
              </div>
              <div className="note-section">
                <h5>👥 Consumer Groups</h5>
                <p>
                  Consumers in the same group share partitions – each partition is read by only one 
                  consumer in the group. Adding/removing consumers triggers a <strong>rebalance</strong>. 
                  Having more consumers than partitions means some consumers will be idle.
                </p>
              </div>
              <div className="note-section">
                <h5>📍 Offset Commits</h5>
                <p>
                  Consumers track their position (offset) in each partition. Committing offsets saves 
                  progress. If a consumer restarts, it resumes from the last committed offset, potentially 
                  re-processing uncommitted messages.
                </p>
              </div>
              <div className="note-section">
                <h5>⚠️ Version Notes</h5>
                <p>
                  Since Kafka 3.0, the default <code>acks</code> changed from <code>1</code> to <code>all</code>, 
                  providing stronger durability guarantees by default.
                </p>
              </div>
            </div>
          )}
        </div>
      )}
    </div>
  );
}
