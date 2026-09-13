/**
 * Kafka Visualizer - Interactive Simulator & Architecture Explorer
 * Accurate 5-Phase Message Flow Engine with Anime.js Motion Paths
 * Inspired by SoftwareMill's Kafka Visualization
 */

(function (window) {
  'use strict';

  const KafkaViz = {
    state: {
      topic: 'orders',
      numPartitions: 3,
      replicationFactor: 3,
      minInsyncReplicas: 2,
      controllerId: 1,
      speed: 1, // 0.6 (Slow/Clear), 1.0 (Normal), 1.6 (Fast)
      isRunningStream: false,
      streamIntervalId: null,
      isAnimating: false,
      currentScenario: 'basic',

      // Step Debugger Phase State: 0=Idle, 1=ProducerSend, 2=LeaderAppend, 3=ISRReplication, 4=HWCommitAck, 5=ConsumerPollCommit
      currentPhase: 0,
      activePendingRecord: null,

      brokers: [
        { id: 1, online: true, isController: true },
        { id: 2, online: true, isController: false },
        { id: 3, online: true, isController: false }
      ],

      // partitionId -> { id, leader, replicas: [], isr: [], records: [{offset, key, val, status}], hw: 0, leo: 0 }
      partitions: {},

      producers: [
        { id: 'P1', key: 'user_101', val: '{"item":"coffee","price":4.5}', partitionTarget: 'auto', acks: 'all' }
      ],

      consumerGroups: [
        {
          id: 'analytics-cg',
          strategy: 'Range',
          consumers: [
            { id: 'C1', online: true, assigned: [0, 1], offsets: { 0: 0, 1: 0 }, committed: { 0: 0, 1: 0 } },
            { id: 'C2', online: true, assigned: [2], offsets: { 2: 0 }, committed: { 2: 0 } }
          ]
        }
      ],

      logs: []
    },

    scenarios: {
      basic: {
        titleEn: '1. Basic Produce & Consume Flow',
        titleVi: '1. Luồng Gửi & Tiêu Thụ Cơ Bản (Basic Flow)',
        descEn: 'Observe the 5-phase lifecycle: (1) Producer Send -> (2) Leader Append & LEO -> (3) ISR Replication -> (4) High Watermark Advance & Producer ACK -> (5) Consumer Poll & Offset Commit.',
        descVi: 'Quan sát trọn vẹn 5 giai đoạn: (1) Producer gửi -> (2) Leader ghi & tăng LEO -> (3) Follower ISR sao chép -> (4) Tăng High Watermark & trả ACK -> (5) Consumer đọc & Commit offset.'
      },
      partitioning: {
        titleEn: '2. Key-based Partitioning (Ordering)',
        titleVi: '2. Phân vùng theo Key (Đảm bảo thứ tự)',
        descEn: 'Messages with the same key hash (murmur2) to the exact same partition, guaranteeing strict FIFO order per key.',
        descVi: 'Các bản ghi có cùng Key sẽ luôn được băm (murmur2) vào cùng một partition, đảm bảo thứ tự xử lý tuyệt đối cho từng key.'
      },
      replication: {
        titleEn: '3. Replication & High Watermark (HW)',
        titleVi: '3. Nhân bản dữ liệu & High Watermark (HW)',
        descEn: 'High Watermark represents the highest offset replicated by all In-Sync Replicas (ISR). Consumers can ONLY read up to the High Watermark.',
        descVi: 'High Watermark (HW) biểu thị offset cao nhất đã được tất cả bản sao trong ISR nhân bản xong. Consumer CHỈ được phép đọc đến HW.'
      },
      failover: {
        titleEn: '4. Broker Crash & Leader Election',
        titleVi: '4. Sự cố Broker & Bầu chọn Leader mới',
        descEn: 'Crash the Leader broker. The Controller detects heartbeat failure, removes it from ISR, and instantly promotes an in-sync follower to new Leader.',
        descVi: 'Khi Leader gặp sự cố, Controller phát hiện mất heartbeat, loại bỏ khỏi ISR và lập tức bầu một follower trong ISR làm Leader mới mà không mất dữ liệu.'
      },
      rebalance: {
        titleEn: '5. Consumer Group Scaling & Rebalance',
        titleVi: '5. Mở rộng Consumer Group & Tái cân bằng',
        descEn: 'Add or remove consumers from a consumer group. Watch Kafka reassign topic partitions across active members dynamically.',
        descVi: 'Thêm hoặc bớt Consumer trong nhóm. Quan sát cơ chế phân chia lại (Rebalance) các partition giữa các consumer đang hoạt động.'
      },
      min_isr: {
        titleEn: '6. acks=all & min.insync.replicas Safety',
        titleVi: '6. Đảm bảo an toàn với acks=all & min.insync.replicas',
        descEn: 'When replicas crash below min.insync.replicas, producing with acks=all fails with NotEnoughReplicasException to prevent data loss.',
        descVi: 'Khi số replica trong ISR thấp hơn min.insync.replicas, lệnh gửi với acks=all sẽ bị từ chối (NotEnoughReplicasException) để chống mất dữ liệu.'
      }
    },

    init() {
      this.resetCluster();
      this.render();
      this.bindEvents();
    },

    resetCluster() {
      this.state.brokers = [
        { id: 1, online: true, isController: true },
        { id: 2, online: true, isController: false },
        { id: 3, online: true, isController: false }
      ];

      this.state.partitions = {};
      for (let p = 0; p < this.state.numPartitions; p++) {
        const leaderId = (p % this.state.brokers.length) + 1;
        this.state.partitions[p] = {
          id: p,
          leader: leaderId,
          replicas: [1, 2, 3],
          isr: [1, 2, 3],
          records: [
            { offset: 0, key: `init_${p}`, val: `{"status":"boot"}`, status: 'committed' }
          ],
          hw: 1,
          leo: 1
        };
      }

      this.state.producers = [
        { id: 'P1', key: 'user_101', val: '{"item":"coffee","price":4.5}', partitionTarget: 'auto', acks: 'all' }
      ];

      this.state.consumerGroups = [
        {
          id: 'analytics-cg',
          strategy: 'Range',
          consumers: [
            { id: 'C1', online: true, assigned: [], offsets: {}, committed: {} },
            { id: 'C2', online: true, assigned: [], offsets: {}, committed: {} }
          ]
        }
      ];

      this.state.currentPhase = 0;
      this.state.activePendingRecord = null;
      this.rebalanceGroup(this.state.consumerGroups[0]);
      this.state.logs = [];
      this.addLog('system', 'SYSTEM', `Kafka Cluster initialized (KRaft Mode, 3 Brokers, Topic: '${this.state.topic}', Partitions: ${this.state.numPartitions}, RF: ${this.state.replicationFactor}, min.insync.replicas: ${this.state.minInsyncReplicas})`);
    },

    rebalanceGroup(group) {
      const activeConsumers = group.consumers.filter(c => c.online);
      const partitionIds = Object.keys(this.state.partitions).map(Number);

      group.consumers.forEach(c => {
        c.assigned = [];
      });

      if (activeConsumers.length === 0) {
        this.addLog('consumer', group.id, `Rebalance complete: No active consumers in group.`);
        return;
      }

      partitionIds.forEach((pId, idx) => {
        const assignedConsumer = activeConsumers[idx % activeConsumers.length];
        assignedConsumer.assigned.push(pId);
        if (assignedConsumer.offsets[pId] === undefined) {
          assignedConsumer.offsets[pId] = this.state.partitions[pId].hw;
          assignedConsumer.committed[pId] = this.state.partitions[pId].hw;
        }
      });

      const summary = activeConsumers.map(c => `${c.id} -> [${c.assigned.map(p => 'P' + p).join(', ')}]`).join(' | ');
      this.addLog('consumer', 'GROUP COORDINATOR', `Rebalance [${group.id}] (${group.strategy}): ${summary}`);
    },

    getPartitionForKey(key) {
      if (!key) return 0;
      let hash = 0;
      for (let i = 0; i < key.length; i++) {
        hash = (hash << 5) - hash + key.charCodeAt(i);
        hash |= 0;
      }
      return Math.abs(hash) % this.state.numPartitions;
    },

    setLifecycleStep(stepNum, descriptionText) {
      const isVi = document.documentElement.lang === 'vi' || (window.CURRENT_LANG && window.CURRENT_LANG === 'vi');
      for (let i = 1; i <= 5; i++) {
        const pill = document.getElementById(`stepPill-${i}`);
        if (pill) {
          pill.classList.remove('active', 'completed');
          if (i < stepNum) {
            pill.classList.add('completed');
          } else if (i === stepNum) {
            pill.classList.add('active');
          }
        }
      }

      const descEl = document.getElementById('vizLifecycleDesc');
      if (descEl) {
        descEl.innerHTML = descriptionText;
        if (window.anime) {
          window.anime({
            targets: descEl,
            opacity: [0.3, 1],
            translateX: [-10, 0],
            duration: 300,
            easing: 'easeOutQuad'
          });
        }
      }
    },

    /* ==========================================================
       SVG BEZIER PATH GENERATOR & ANIME.JS MOTION LAYER
       ========================================================== */
    drawConnectionCurves() {
      const svg = document.getElementById('vizSvgLayer');
      if (!svg) return;

      const stage = document.querySelector('.viz-stage');
      if (!stage) return;

      const stageRect = stage.getBoundingClientRect();
      svg.setAttribute('viewBox', `0 0 ${stageRect.width} ${stageRect.height}`);
      svg.innerHTML = '';

      // 1. Producer -> Partition Leaders Curves
      this.state.producers.forEach(p => {
        const prodEl = document.getElementById(`producer-${p.id}`);
        if (!prodEl) return;
        const pRect = prodEl.getBoundingClientRect();
        const startX = pRect.right - stageRect.left;
        const startY = pRect.top + pRect.height / 2 - stageRect.top;

        Object.values(this.state.partitions).forEach(part => {
          const leaderEl = document.getElementById(`part-${part.leader}-${part.id}`);
          if (!leaderEl) return;
          const lRect = leaderEl.getBoundingClientRect();
          const endX = lRect.left - stageRect.left;
          const endY = lRect.top + lRect.height / 2 - stageRect.top;

          const path = document.createElementNS('http://www.w3.org/2000/svg', 'path');
          const cp1X = startX + (endX - startX) * 0.5;
          const cp1Y = startY;
          const cp2X = startX + (endX - startX) * 0.5;
          const cp2Y = endY;
          path.setAttribute('d', `M ${startX} ${startY} C ${cp1X} ${cp1Y}, ${cp2X} ${cp2Y}, ${endX} ${endY}`);
          path.setAttribute('class', 'viz-svg-path');
          path.id = `path-prod-${p.id}-part-${part.id}`;
          svg.appendChild(path);
        });
      });

      // 2. Intra-Cluster Replication Curves (Leader -> Followers)
      Object.values(this.state.partitions).forEach(part => {
        const leaderEl = document.getElementById(`part-${part.leader}-${part.id}`);
        if (!leaderEl) return;
        const lRect = leaderEl.getBoundingClientRect();
        const startX = lRect.left + lRect.width / 2 - stageRect.left;
        const startY = lRect.top + lRect.height / 2 - stageRect.top;

        part.replicas.forEach(fId => {
          if (fId === part.leader) return;
          const followerEl = document.getElementById(`part-${fId}-${part.id}`);
          if (!followerEl) return;
          const fRect = followerEl.getBoundingClientRect();
          const endX = fRect.left + fRect.width / 2 - stageRect.left;
          const endY = fRect.top + fRect.height / 2 - stageRect.top;

          const path = document.createElementNS('http://www.w3.org/2000/svg', 'path');
          const midY = (startY + endY) / 2;
          const cp1X = startX + (endX > startX ? 45 : -45);
          path.setAttribute('d', `M ${startX} ${startY} Q ${cp1X} ${midY}, ${endX} ${endY}`);
          path.setAttribute('class', 'viz-svg-path');
          path.id = `path-repl-p${part.id}-${part.leader}-to-${fId}`;
          svg.appendChild(path);
        });
      });

      // 3. Leader Partitions -> Active Assigned Consumers Curves
      this.state.consumerGroups.forEach(g => {
        g.consumers.forEach(c => {
          if (!c.online) return;
          const consEl = document.getElementById(`consumer-${c.id}`);
          if (!consEl) return;
          const cRect = consEl.getBoundingClientRect();
          const endX = cRect.left - stageRect.left;
          const endY = cRect.top + cRect.height / 2 - stageRect.top;

          c.assigned.forEach(pId => {
            const part = this.state.partitions[pId];
            if (!part) return;
            const leaderEl = document.getElementById(`part-${part.leader}-${part.id}`);
            if (!leaderEl) return;
            const lRect = leaderEl.getBoundingClientRect();
            const startX = lRect.right - stageRect.left;
            const startY = lRect.top + lRect.height / 2 - stageRect.top;

            const path = document.createElementNS('http://www.w3.org/2000/svg', 'path');
            const cp1X = startX + (endX - startX) * 0.5;
            const cp1Y = startY;
            const cp2X = startX + (endX - startX) * 0.5;
            const cp2Y = endY;
            path.setAttribute('d', `M ${startX} ${startY} C ${cp1X} ${cp1Y}, ${cp2X} ${cp2Y}, ${endX} ${endY}`);
            path.setAttribute('class', 'viz-svg-path');
            path.id = `path-part-${part.id}-cons-${c.id}`;
            svg.appendChild(path);
          });
        });
      });
    },

    /* ==========================================================
       ANIME.JS MOTION PATH FLIGHT DISPATCHER
       ========================================================== */
    animateFlightWithPath(svgPathId, options = {}) {
      return new Promise(resolve => {
        const overlay = document.getElementById('vizPacketOverlay');
        const svgPath = document.getElementById(svgPathId);

        if (!overlay) {
          resolve();
          return;
        }

        const packet = document.createElement('div');
        packet.className = `viz-flying-packet ${options.type || 'produce'}`;
        packet.innerHTML = options.label || '✉️';
        overlay.appendChild(packet);

        if (svgPath) {
          svgPath.classList.add('active');
        }

        // Pacing: Clear, observable speed (default ~900ms per phase)
        const duration = (options.duration || 900) / this.state.speed;

        if (window.anime && svgPath) {
          const path = window.anime.path(svgPath);
          const isReverse = !!options.reverse;

          window.anime({
            targets: packet,
            translateX: isReverse ? [path('x')(1), path('x')(0)] : path('x'),
            translateY: isReverse ? [path('y')(1), path('y')(0)] : path('y'),
            rotate: isReverse ? 0 : path('angle'),
            scale: [0.75, 1.1, 1],
            easing: 'easeInOutCubic',
            duration: duration,
            complete: () => {
              packet.remove();
              if (svgPath) svgPath.classList.remove('active');
              resolve();
            }
          });
        } else {
          // Fallback coordinate animation
          const stage = document.querySelector('.viz-stage');
          const stageRect = stage ? stage.getBoundingClientRect() : { left: 0, top: 0 };
          const fromRect = options.fromEl ? options.fromEl.getBoundingClientRect() : { left: 0, top: 0, width: 0, height: 0 };
          const toRect = options.toEl ? options.toEl.getBoundingClientRect() : { left: 100, top: 100, width: 0, height: 0 };

          const startX = fromRect.left + fromRect.width / 2 - stageRect.left;
          const startY = fromRect.top + fromRect.height / 2 - stageRect.top;
          const endX = toRect.left + toRect.width / 2 - stageRect.left;
          const endY = toRect.top + toRect.height / 2 - stageRect.top;

          packet.style.left = `${startX}px`;
          packet.style.top = `${startY}px`;

          if (window.anime) {
            window.anime({
              targets: packet,
              left: [`${startX}px`, `${endX}px`],
              top: [`${startY}px`, `${endY}px`],
              scale: [0.75, 1.1, 1],
              easing: 'easeInOutQuad',
              duration: duration,
              complete: () => {
                packet.remove();
                if (svgPath) svgPath.classList.remove('active');
                resolve();
              }
            });
          } else {
            setTimeout(() => {
              packet.remove();
              if (svgPath) svgPath.classList.remove('active');
              resolve();
            }, duration);
          }
        }
      });
    },

    /* ==========================================================
       CONCISE 5-PHASE MESSAGE LIFECYCLE (PRODUCER -> KAFKA -> CONSUMER)
       ========================================================== */
    async executeFullMessageCycle(producerId) {
      if (this.state.isAnimating) return;
      this.state.isAnimating = true;

      const isVi = document.documentElement.lang === 'vi' || (window.CURRENT_LANG && window.CURRENT_LANG === 'vi');

      try {
        const producer = this.state.producers.find(p => p.id === producerId);
        if (!producer) return;

        let targetPartId;
        if (producer.partitionTarget === 'auto') {
          targetPartId = this.getPartitionForKey(producer.key);
        } else {
          targetPartId = parseInt(producer.partitionTarget, 10);
        }

        const partition = this.state.partitions[targetPartId];
        if (!partition) return;

        const leaderBroker = this.state.brokers.find(b => b.id === partition.leader);
        if (!leaderBroker || !leaderBroker.online) {
          this.addLog('failover', producer.id, `ProduceRequest failed: Leader for Partition ${targetPartId} is OFFLINE (LeaderNotAvailableException)`);
          this.triggerFlashMessage(`❌ Produce failed: Leader for Partition ${targetPartId} is OFFLINE!`, 'error');
          this.setLifecycleStep(0, isVi ? '❌ Thất bại: Broker Leader đang Offline!' : '❌ Failed: Partition Leader is Offline!');
          return;
        }

        // Check min.insync.replicas
        if (producer.acks === 'all' && partition.isr.length < this.state.minInsyncReplicas) {
          this.addLog('failover', producer.id, `ProduceRequest rejected: ISR (${partition.isr.length}) < min.insync.replicas (${this.state.minInsyncReplicas}) -> NotEnoughReplicasException`);
          this.triggerFlashMessage(`❌ Rejected: ISR (${partition.isr.length}) < min.insync.replicas (${this.state.minInsyncReplicas})`, 'error');
          this.setLifecycleStep(0, isVi ? `❌ Bị từ chối: ISR (${partition.isr.length}) < min.insync.replicas (${this.state.minInsyncReplicas})!` : `❌ Rejected: ISR count (${partition.isr.length}) < min.insync.replicas (${this.state.minInsyncReplicas})!`);
          return;
        }

        const prodEl = document.getElementById(`producer-${producer.id}`);
        const leaderPartEl = document.getElementById(`part-${partition.leader}-${partition.id}`);
        const newOffset = partition.leo;

        // -------------------------------------------------------------
        // PHASE 1: PRODUCER SEND
        // -------------------------------------------------------------
        this.setLifecycleStep(1, isVi
          ? `<strong>[Giai đoạn 1/5]</strong> Producer <code>${producer.id}</code> gửi bản ghi <code>[Key: ${producer.key}]</code> tới <strong>Broker ${leaderBroker.id} (Leader P${targetPartId})</strong>...`
          : `<strong>[Phase 1/5]</strong> Producer <code>${producer.id}</code> sending record <code>[Key: ${producer.key}]</code> to <strong>Broker ${leaderBroker.id} (Leader Partition ${targetPartId})</strong>...`
        );
        this.addLog('producer', producer.id, `ProduceRequest -> Topic '${this.state.topic}', P${targetPartId}, Key='${producer.key}', Acks=${producer.acks}`);

        if (window.anime && prodEl) {
          window.anime({
            targets: prodEl,
            scale: [1, 1.04, 1],
            borderColor: ['var(--border-color)', '#6366f1', 'var(--border-color)'],
            duration: 600
          });
        }

        const prodPathId = `path-prod-${producer.id}-part-${targetPartId}`;
        await this.animateFlightWithPath(prodPathId, {
          type: 'produce',
          label: `✉️ Record [P${targetPartId}: ${producer.key}]`,
          duration: 950,
          fromEl: prodEl,
          toEl: leaderPartEl
        });

        await this.sleep(300 / this.state.speed);

        // -------------------------------------------------------------
        // PHASE 2: LEADER APPEND & LEO INCREMENT (UNCOMMITTED)
        // -------------------------------------------------------------
        this.setLifecycleStep(2, isVi
          ? `<strong>[Giai đoạn 2/5]</strong> Broker ${leaderBroker.id} (Leader) ghi bản ghi vào Log tại <strong>Offset #${newOffset}</strong>. Trạng thái: <em>Chưa commit (Uncommitted)</em>. LEO = ${newOffset + 1}, HW = ${partition.hw}.`
          : `<strong>[Phase 2/5]</strong> Broker ${leaderBroker.id} (Leader) appended record at <strong>Offset #${newOffset}</strong>. Status: <em>Uncommitted</em>. LEO = ${newOffset + 1}, HW = ${partition.hw}.`
        );

        const newRecord = {
          offset: newOffset,
          key: producer.key,
          val: producer.val,
          status: producer.acks === '0' ? 'committed' : 'uncommitted'
        };
        partition.records.push(newRecord);
        partition.leo = newOffset + 1;

        this.highlightNode(`broker-${leaderBroker.id}`);
        this.render();
        this.animateLogCellPop(`rec-${partition.id}-${newOffset}`);
        this.addLog('leader', `BROKER ${leaderBroker.id}`, `Leader P${targetPartId} appended record at offset ${newOffset} (LEO=${partition.leo})`);

        await this.sleep(500 / this.state.speed);

        // -------------------------------------------------------------
        // PHASE 3: ISR FOLLOWER REPLICATION
        // -------------------------------------------------------------
        const onlineFollowers = partition.replicas.filter(rId => {
          const b = this.state.brokers.find(br => br.id === rId);
          return rId !== partition.leader && b && b.online;
        });

        if (onlineFollowers.length > 0) {
          this.setLifecycleStep(3, isVi
            ? `<strong>[Giai đoạn 3/5]</strong> Các Follower <strong>Broker [${onlineFollowers.join(', ')}]</strong> trong ISR sao chép bản ghi Offset #${newOffset} và gửi phản hồi (ACK) về Leader.`
            : `<strong>[Phase 3/5]</strong> Follower brokers <strong>[${onlineFollowers.join(', ')}]</strong> in ISR replicate Offset #${newOffset} and send ACK back to Leader.`
          );

          const replPromises = onlineFollowers.map(async fId => {
            const followerPartEl = document.getElementById(`part-${fId}-${partition.id}`);
            const followerBrokerEl = document.getElementById(`broker-${fId}`);
            const replPathId = `path-repl-p${partition.id}-${partition.leader}-to-${fId}`;

            if (followerBrokerEl && window.anime) {
              window.anime({
                targets: followerBrokerEl,
                borderColor: ['var(--border-color)', '#06b6d4', 'var(--border-color)'],
                duration: 700
              });
            }

            // Flight Leader -> Follower
            await this.animateFlightWithPath(replPathId, {
              type: 'replicate',
              label: `🔄 Replicate (off:#${newOffset})`,
              duration: 800,
              fromEl: leaderPartEl,
              toEl: followerPartEl
            });

            this.addLog('isr', `BROKER ${fId}`, `Follower replicated P${targetPartId} offset ${newOffset}`);

            // Follower ACK -> Leader (reverse path)
            await this.animateFlightWithPath(replPathId, {
              type: 'ack',
              label: `✓ ACK Replicated`,
              duration: 550,
              reverse: true,
              fromEl: followerPartEl,
              toEl: leaderPartEl
            });
          });

          await Promise.all(replPromises);
        }

        await this.sleep(300 / this.state.speed);

        // -------------------------------------------------------------
        // PHASE 4: HIGH WATERMARK ADVANCE & PRODUCER ACK
        // -------------------------------------------------------------
        if (partition.isr.length >= (producer.acks === 'all' ? this.state.minInsyncReplicas : 1)) {
          partition.hw = partition.leo;
          newRecord.status = 'committed';
          this.render();
          this.addLog('isr', `PARTITION ${targetPartId}`, `All ISR [${partition.isr.join(',')}] caught up -> High Watermark advanced to ${partition.hw}`);
        }

        this.setLifecycleStep(4, isVi
          ? `<strong>[Giai đoạn 4/5]</strong> Tất cả bản sao ISR đã đồng bộ -> <strong>High Watermark (HW) tiến lên ${partition.hw}</strong>. Bản ghi chuyển sang <strong>COMMITTED</strong>. Leader gửi ACK về Producer.`
          : `<strong>[Phase 4/5]</strong> All ISR in sync -> <strong>High Watermark (HW) advanced to ${partition.hw}</strong>. Record is <strong>COMMITTED</strong>. Leader returns ACK to Producer.`
        );

        if (producer.acks !== '0') {
          await this.animateFlightWithPath(prodPathId, {
            type: 'ack',
            label: `✅ ACK OK (P${targetPartId}:#${newOffset})`,
            duration: 750,
            reverse: true,
            fromEl: leaderPartEl,
            toEl: prodEl
          });
        }
        this.addLog('producer', producer.id, `ProduceResponse -> ACK OK (Topic='${this.state.topic}', Partition=${targetPartId}, Offset=${newOffset})`);

        await this.sleep(500 / this.state.speed);

        // -------------------------------------------------------------
        // PHASE 5: CONSUMER POLL, DELIVER & COMMIT OFFSET
        // -------------------------------------------------------------
        const group = this.state.consumerGroups[0];
        const assignedConsumer = group.consumers.find(c => c.online && c.assigned.includes(targetPartId));

        if (assignedConsumer) {
          const consEl = document.getElementById(`consumer-${assignedConsumer.id}`);
          const consPathId = `path-part-${targetPartId}-cons-${assignedConsumer.id}`;

          this.setLifecycleStep(5, isVi
            ? `<strong>[Giai đoạn 5/5]</strong> Consumer <code>${assignedConsumer.id}</code> gọi <code>poll()</code> -> Nhận bản ghi <strong>Offset #${newOffset}</strong> từ Leader và commit <strong>Offset #${newOffset + 1}</strong> vào <code>__consumer_offsets</code>.`
            : `<strong>[Phase 5/5]</strong> Consumer <code>${assignedConsumer.id}</code> calls <code>poll()</code> -> Fetches <strong>Offset #${newOffset}</strong> from Leader and commits <strong>Offset #${newOffset + 1}</strong> to <code>__consumer_offsets</code>.`
          );

          if (consEl && window.anime) {
            window.anime({
              targets: consEl,
              scale: [1, 1.04, 1],
              borderColor: ['var(--border-color)', '#10b981', 'var(--border-color)'],
              duration: 700
            });
          }

          // Fetch Record Leader -> Consumer along SVG path
          await this.animateFlightWithPath(consPathId, {
            type: 'fetch',
            label: `📥 Fetch (P${targetPartId}:#${newOffset})`,
            duration: 850,
            fromEl: leaderPartEl,
            toEl: consEl
          });

          assignedConsumer.offsets[targetPartId] = newOffset + 1;
          assignedConsumer.committed[targetPartId] = newOffset + 1;
          this.addLog('consumer', `${group.id}:${assignedConsumer.id}`, `Fetched P${targetPartId} Offset ${newOffset} [Key='${newRecord.key}']`);

          // Offset Commit Consumer -> Coordinator (reverse along SVG path)
          await this.animateFlightWithPath(consPathId, {
            type: 'commit',
            label: `📌 Commit (P${targetPartId}:#${newOffset + 1})`,
            duration: 650,
            reverse: true,
            fromEl: consEl,
            toEl: leaderPartEl
          });

          this.addLog('consumer', `${group.id}:${assignedConsumer.id}`, `OffsetCommitRequest P${targetPartId} -> Committed offset ${assignedConsumer.committed[targetPartId]} to __consumer_offsets (Lag=0)`);
        } else {
          this.setLifecycleStep(5, isVi
            ? `<strong>[Giai đoạn 5/5]</strong> Không có Consumer nào đang online được gán Partition ${targetPartId}. Bản ghi tồn tại trong log và sẵn sàng khi Consumer kết nối.`
            : `<strong>[Phase 5/5]</strong> No active consumer assigned to Partition ${targetPartId}. Record remains stored in log ready for consumption.`
          );
        }

        this.render();
      } finally {
        this.state.isAnimating = false;
      }
    },

    animateLogCellPop(cellId) {
      if (window.anime) {
        const el = document.getElementById(cellId);
        if (el) {
          window.anime({
            targets: el,
            scale: [0.1, 1.18, 1],
            opacity: [0, 1],
            easing: 'easeOutElastic(1, .7)',
            duration: 500 / this.state.speed
          });
        }
      }
    },

    toggleBroker(brokerId) {
      const broker = this.state.brokers.find(b => b.id === brokerId);
      if (!broker) return;

      broker.online = !broker.online;
      const brokerEl = document.getElementById(`broker-${brokerId}`);

      if (!broker.online) {
        this.addLog('failover', 'CLUSTER', `Broker ${brokerId} CRASHED / STOPPED.`);

        if (window.anime && brokerEl) {
          window.anime({
            targets: brokerEl,
            translateX: [0, -10, 10, -6, 6, -2, 2, 0],
            opacity: [1, 0.55],
            easing: 'easeInOutSine',
            duration: 450
          });
        }

        // Remove from ISRs
        Object.values(this.state.partitions).forEach(part => {
          part.isr = part.isr.filter(id => id !== brokerId);

          // If leader was crashed broker, elect new leader from ISR
          if (part.leader === brokerId) {
            if (part.isr.length > 0) {
              const newLeader = part.isr[0];
              part.leader = newLeader;
              this.addLog('failover', 'CONTROLLER', `Leader failover for P${part.id}: Broker ${brokerId} -> Broker ${newLeader} (New Leader elected from ISR [${part.isr.join(',')}])`);
            } else {
              part.leader = null;
              this.addLog('failover', 'CONTROLLER', `CRITICAL: Partition ${part.id} has NO available In-Sync Replicas! Partition is Offline.`);
            }
          }
        });
      } else {
        this.addLog('failover', 'CLUSTER', `Broker ${brokerId} RECOVERED & RESTARTED.`);

        if (window.anime && brokerEl) {
          window.anime({
            targets: brokerEl,
            scale: [0.95, 1.02, 1],
            opacity: [0.55, 1],
            easing: 'easeOutElastic(1, .8)',
            duration: 500
          });
        }

        // Recover into ISR
        Object.values(this.state.partitions).forEach(part => {
          if (part.replicas.includes(brokerId) && !part.isr.includes(brokerId)) {
            part.isr.push(brokerId);
            part.isr.sort();
            this.addLog('isr', 'LEADER', `Broker ${brokerId} caught up with log -> Re-joined ISR for P${part.id} [${part.isr.join(',')}]`);
          }
          if (part.leader === null && part.isr.length > 0) {
            part.leader = part.isr[0];
            this.addLog('failover', 'CONTROLLER', `Partition ${part.id} restored: Elected Broker ${part.leader} as Leader.`);
          }
        });
      }

      this.render();
    },

    toggleConsumer(groupId, consumerId) {
      const group = this.state.consumerGroups.find(g => g.id === groupId);
      if (!group) return;
      const consumer = group.consumers.find(c => c.id === consumerId);
      if (!consumer) return;

      consumer.online = !consumer.online;
      this.addLog('consumer', consumer.id, `Consumer state changed to: ${consumer.online ? 'ONLINE' : 'OFFLINE (Crashed/Left)'}`);
      this.rebalanceGroup(group);
      this.render();
    },

    addConsumer(groupId) {
      const group = this.state.consumerGroups.find(g => g.id === groupId);
      if (!group) return;
      const nextNum = group.consumers.length + 1;
      const newId = `C${nextNum}`;
      group.consumers.push({
        id: newId,
        online: true,
        assigned: [],
        offsets: {},
        committed: {}
      });
      this.addLog('consumer', 'GROUP COORDINATOR', `New member joined [${group.id}]: Consumer ${newId}`);
      this.rebalanceGroup(group);
      this.render();
    },

    applyScenario(scenarioKey) {
      this.state.currentScenario = scenarioKey;
      this.resetCluster();

      if (scenarioKey === 'partitioning') {
        this.state.producers[0].key = 'user_order_99';
        this.state.producers[0].val = '{"orderId":99,"amount":250}';
      } else if (scenarioKey === 'replication') {
        this.state.producers[0].acks = 'all';
      } else if (scenarioKey === 'failover') {
        this.addLog('failover', 'SCENARIO', 'Scenario 4: Click "Crash Broker" on Broker 1 to trigger instant Leader election!');
      } else if (scenarioKey === 'rebalance') {
        this.addConsumer('analytics-cg');
      } else if (scenarioKey === 'min_isr') {
        this.state.minInsyncReplicas = 2;
        this.state.producers[0].acks = 'all';
      }

      this.render();
    },

    addLog(tag, source, msg) {
      const d = new Date();
      const timeStr = `${String(d.getMinutes()).padStart(2, '0')}:${String(d.getSeconds()).padStart(2, '0')}.${String(Math.floor(d.getMilliseconds() / 10)).padStart(2, '0')}`;
      this.state.logs.unshift({ time: timeStr, tag, source, msg });
      if (this.state.logs.length > 80) this.state.logs.pop();

      const logContainer = document.getElementById('vizTerminalLogs');
      if (logContainer) {
        logContainer.innerHTML = this.renderLogsHtml();
      }
    },

    renderLogsHtml() {
      if (this.state.logs.length === 0) {
        return `<div style="color: #64748b; font-style: italic;">No protocol events yet. Send a record or trigger a broker action to observe Kafka activity.</div>`;
      }
      return this.state.logs.map(log => `
        <div class="viz-log-line">
          <span class="viz-log-time">${log.time}</span>
          <span class="viz-log-tag ${log.tag}">[${log.source}]</span>
          <span class="viz-log-msg">${escapeHtml(log.msg)}</span>
        </div>
      `).join('');
    },

    toggleStream() {
      this.state.isRunningStream = !this.state.isRunningStream;
      const btn = document.getElementById('vizStreamToggleBtn');

      if (this.state.isRunningStream) {
        if (btn) btn.innerHTML = '⏸ <span id="vizStreamLabel">Pause Stream</span>';
        this.state.streamIntervalId = setInterval(async () => {
          if (this.state.isAnimating) return;
          const keys = ['user_101', 'user_202', 'order_505', 'sensor_99', 'item_33'];
          const randKey = keys[Math.floor(Math.random() * keys.length)];
          this.state.producers[0].key = randKey;
          this.state.producers[0].val = JSON.stringify({ event: 'evt_' + Math.floor(Math.random() * 1000), ts: Date.now() });

          await this.executeFullMessageCycle(this.state.producers[0].id);
        }, 5500 / this.state.speed);
      } else {
        if (btn) btn.innerHTML = '▶ <span id="vizStreamLabel">Start Stream</span>';
        if (this.state.streamIntervalId) {
          clearInterval(this.state.streamIntervalId);
          this.state.streamIntervalId = null;
        }
      }
    },

    async stepForward() {
      await this.executeFullMessageCycle(this.state.producers[0].id);
    },

    render() {
      const container = document.getElementById('visualizerContainer');
      if (!container) return;

      const isVi = document.documentElement.lang === 'vi' || (window.CURRENT_LANG && window.CURRENT_LANG === 'vi');
      const sc = this.scenarios[this.state.currentScenario] || this.scenarios.basic;
      const scTitle = isVi ? sc.titleVi : sc.titleEn;
      const scDesc = isVi ? sc.descVi : sc.descEn;

      let totalRecords = 0;
      let totalLag = 0;
      Object.values(this.state.partitions).forEach(p => {
        totalRecords += p.records.length;
      });

      this.state.consumerGroups.forEach(g => {
        g.consumers.forEach(c => {
          c.assigned.forEach(pId => {
            const p = this.state.partitions[pId];
            if (p) {
              const offset = c.offsets[pId] || 0;
              totalLag += Math.max(0, p.hw - offset);
            }
          });
        });
      });

      container.innerHTML = `
        <div class="viz-container">
          <!-- SVG Connection Curves & Flight Overlay Layer -->
          <svg id="vizSvgLayer"></svg>
          <div id="vizPacketOverlay"></div>

          <!-- Control Toolbar Card -->
          <div class="viz-toolbar-card">
            <div class="viz-toolbar-top">
              <div class="viz-scenario-group">
                <label style="font-weight: 700; font-size: 0.9rem; white-space: nowrap;">📖 ${isVi ? 'Kịch bản Mẫu' : 'Preset Scenario'}:</label>
                <select id="vizScenarioSelect" class="viz-select">
                  <option value="basic" ${this.state.currentScenario === 'basic' ? 'selected' : ''}>${isVi ? this.scenarios.basic.titleVi : this.scenarios.basic.titleEn}</option>
                  <option value="partitioning" ${this.state.currentScenario === 'partitioning' ? 'selected' : ''}>${isVi ? this.scenarios.partitioning.titleVi : this.scenarios.partitioning.titleEn}</option>
                  <option value="replication" ${this.state.currentScenario === 'replication' ? 'selected' : ''}>${isVi ? this.scenarios.replication.titleVi : this.scenarios.replication.titleEn}</option>
                  <option value="failover" ${this.state.currentScenario === 'failover' ? 'selected' : ''}>${isVi ? this.scenarios.failover.titleVi : this.scenarios.failover.titleEn}</option>
                  <option value="rebalance" ${this.state.currentScenario === 'rebalance' ? 'selected' : ''}>${isVi ? this.scenarios.rebalance.titleVi : this.scenarios.rebalance.titleEn}</option>
                  <option value="min_isr" ${this.state.currentScenario === 'min_isr' ? 'selected' : ''}>${isVi ? this.scenarios.min_isr.titleVi : this.scenarios.min_isr.titleEn}</option>
                </select>
              </div>

              <div class="viz-controls-group">
                <button id="vizStreamToggleBtn" class="viz-btn primary">
                  ${this.state.isRunningStream ? '⏸' : '▶'} <span>${this.state.isRunningStream ? (isVi ? 'Tạm Dừng Luồng' : 'Pause Stream') : (isVi ? 'Chạy Tự Động (Stream)' : 'Auto Stream')}</span>
                </button>
                <button id="vizStepBtn" class="viz-btn" title="Step forward full 5-phase message cycle">
                  ⏭ <span>${isVi ? 'Từng Bước (Step)' : 'Next Step'}</span>
                </button>
                <button id="vizResetBtn" class="viz-btn danger" title="Reset cluster">
                  🔄 <span>${isVi ? 'Đặt Lại' : 'Reset Cluster'}</span>
                </button>

                <div class="viz-speed-wrapper">
                  <span>⚡ ${isVi ? 'Tốc độ' : 'Speed'}:</span>
                  <select id="vizSpeedSelect" class="viz-form-select" style="padding: 0.25rem 0.5rem; font-size: 0.8rem;">
                    <option value="0.6" ${this.state.speed === 0.6 ? 'selected' : ''}>0.6x (${isVi ? 'Chậm & Chi tiết' : 'Slow / Educational'})</option>
                    <option value="1" ${this.state.speed === 1 ? 'selected' : ''}>1.0x (${isVi ? 'Bình thường' : 'Normal'})</option>
                    <option value="1.6" ${this.state.speed === 1.6 ? 'selected' : ''}>1.6x (${isVi ? 'Nhanh' : 'Fast'})</option>
                  </select>
                </div>
              </div>
            </div>

            <!-- Scenario explanation banner -->
            <div class="viz-scenario-banner">
              <span class="icon">💡</span>
              <div>
                <strong>${scTitle}</strong>: ${scDesc}
              </div>
            </div>
          </div>

          <!-- Live 5-Phase Lifecycle Stepper HUD -->
          <div class="viz-lifecycle-hud">
            <div class="viz-stepper">
              <div class="viz-step-pill" id="stepPill-1"><span class="step-num">1</span> <span class="step-label">${isVi ? 'Producer Gửi (Send)' : 'Producer Send'}</span></div>
              <div class="viz-step-arrow">→</div>
              <div class="viz-step-pill" id="stepPill-2"><span class="step-num">2</span> <span class="step-label">${isVi ? 'Leader Ghi Log (LEO)' : 'Leader Append (LEO)'}</span></div>
              <div class="viz-step-arrow">→</div>
              <div class="viz-step-pill" id="stepPill-3"><span class="step-num">3</span> <span class="step-label">${isVi ? 'Follower Sao Chép (ISR)' : 'ISR Replication'}</span></div>
              <div class="viz-step-arrow">→</div>
              <div class="viz-step-pill" id="stepPill-4"><span class="step-num">4</span> <span class="step-label">${isVi ? 'Tăng HW & Trả ACK' : 'HW Commit & ACK'}</span></div>
              <div class="viz-step-arrow">→</div>
              <div class="viz-step-pill" id="stepPill-5"><span class="step-num">5</span> <span class="step-label">${isVi ? 'Consumer Đọc & Commit' : 'Consumer Poll & Commit'}</span></div>
            </div>
            <div class="viz-lifecycle-desc" id="vizLifecycleDesc">
              ${isVi ? 'Sẵn sàng. Nhấn "Gửi Bản Ghi" hoặc "Từng Bước" để theo dõi luồng tin nhắn chính xác qua từng giai đoạn.' : 'Ready. Click "Send Record" or "Next Step" to trace exact message delivery across all 5 phases.'}
            </div>
          </div>

          <!-- Top Metric Cards -->
          <div class="viz-metrics-bar">
            <div class="viz-metric-card">
              <span class="viz-metric-label">${isVi ? 'Topic Đang Chọn' : 'Active Topic'}</span>
              <span class="viz-metric-value">📦 ${this.state.topic}</span>
            </div>
            <div class="viz-metric-card">
              <span class="viz-metric-label">${isVi ? 'Số Partitions' : 'Partitions'}</span>
              <span class="viz-metric-value">🔢 ${this.state.numPartitions} (RF: ${this.state.replicationFactor})</span>
            </div>
            <div class="viz-metric-card">
              <span class="viz-metric-label">${isVi ? 'Min In-Sync Replicas' : 'min.insync.replicas'}</span>
              <span class="viz-metric-value">🛡️ ${this.state.minInsyncReplicas}</span>
            </div>
            <div class="viz-metric-card">
              <span class="viz-metric-label">${isVi ? 'Tổng Bản Ghi' : 'Total Records'}</span>
              <span class="viz-metric-value">📝 ${totalRecords}</span>
            </div>
            <div class="viz-metric-card">
              <span class="viz-metric-label">${isVi ? 'Tổng Consumer Lag' : 'Consumer Total Lag'}</span>
              <span class="viz-metric-value" style="color: ${totalLag > 0 ? '#f59e0b' : '#10b981'};">⏳ ${totalLag} msgs</span>
            </div>
          </div>

          <!-- 3-Column Topology Stage -->
          <div class="viz-stage">
            <!-- Left Column: Producers -->
            <div class="viz-column">
              <div class="viz-col-header">
                <span class="viz-col-title">📤 ${isVi ? 'Producers (Gửi tin)' : 'Producers'}</span>
              </div>

              ${this.state.producers.map(p => `
                <div class="viz-producer-card" id="producer-${p.id}">
                  <div class="viz-card-head">
                    <span class="viz-card-title">🚀 Producer (${p.id})</span>
                    <span class="viz-badge leader">acks=${p.acks}</span>
                  </div>

                  <div class="viz-form-row">
                    <label>${isVi ? 'Khóa Tin Nhắn (Key)' : 'Message Key (murmur2 hash)'}:</label>
                    <input type="text" class="viz-input" id="pKey-${p.id}" value="${escapeHtml(p.key)}" placeholder="e.g. user_101" />
                  </div>

                  <div class="viz-form-row">
                    <label>${isVi ? 'Nội dung (Value JSON)' : 'Payload (Value)'}:</label>
                    <input type="text" class="viz-input" id="pVal-${p.id}" value="${escapeHtml(p.val)}" placeholder='{"price": 4.5}' />
                  </div>

                  <div class="viz-form-row">
                    <label>${isVi ? 'Đích đến Partition' : 'Target Partition'}:</label>
                    <select class="viz-form-select" id="pPart-${p.id}">
                      <option value="auto" ${p.partitionTarget === 'auto' ? 'selected' : ''}>⚡ Auto (Key Murmur2 Hash)</option>
                      ${Object.keys(this.state.partitions).map(pId => `
                        <option value="${pId}" ${p.partitionTarget === String(pId) ? 'selected' : ''}>Partition ${pId}</option>
                      `).join('')}
                    </select>
                  </div>

                  <div class="viz-form-row">
                    <label>${isVi ? 'Cấu hình acks' : 'Acks Configuration'}:</label>
                    <select class="viz-form-select" id="pAcks-${p.id}">
                      <option value="all" ${p.acks === 'all' ? 'selected' : ''}>acks = all / -1 (Leader + Full ISR)</option>
                      <option value="1" ${p.acks === '1' ? 'selected' : ''}>acks = 1 (Leader Only)</option>
                      <option value="0" ${p.acks === '0' ? 'selected' : ''}>acks = 0 (Fire & Forget, No ACK)</option>
                    </select>
                  </div>

                  <button class="viz-btn primary" onclick="KafkaViz.handleProduceClick('${p.id}')" style="margin-top: 0.35rem; justify-content: center;">
                    📤 ${isVi ? 'Gửi Bản Ghi (Animate Flow)' : 'Send Record (Animate Flow)'}
                  </button>
                </div>
              `).join('')}
            </div>

            <!-- Center Column: Brokers Cluster -->
            <div class="viz-column">
              <div class="viz-col-header">
                <span class="viz-col-title">🖥️ ${isVi ? 'Kafka Brokers & Replicated Partitions' : 'Kafka Brokers & Partitions'}</span>
              </div>

              <div class="viz-cluster-box">
                <div class="viz-brokers-grid">
                  ${this.state.brokers.map(broker => `
                    <div class="viz-broker-node ${broker.online ? '' : 'crashed'}" id="broker-${broker.id}">
                      <div class="viz-broker-header">
                        <div class="viz-broker-id">
                          <span>📦</span> Broker ${broker.id}
                          ${broker.isController ? `<span class="viz-badge controller">KRaft Controller</span>` : ''}
                        </div>
                        <div style="display: flex; gap: 0.4rem; align-items: center;">
                          <span class="viz-badge ${broker.online ? 'online' : 'offline'}">${broker.online ? 'ONLINE' : 'CRASHED'}</span>
                          <button class="viz-btn ${broker.online ? 'danger' : 'success'}" style="padding: 0.15rem 0.45rem; font-size: 0.72rem;" onclick="KafkaViz.toggleBroker(${broker.id})">
                            ${broker.online ? (isVi ? '🔴 Tắt' : '🔴 Crash') : (isVi ? '🟢 Bật' : '🟢 Start')}
                          </button>
                        </div>
                      </div>

                      <!-- Partitions hosted on this broker -->
                      ${Object.values(this.state.partitions).map(part => {
                        const isLeader = part.leader === broker.id;
                        const isReplica = part.replicas.includes(broker.id);
                        const isInISR = part.isr.includes(broker.id);
                        if (!isReplica) return '';

                        return `
                          <div class="viz-partition-box ${isLeader ? 'leader-box' : ''}" id="part-${broker.id}-${part.id}">
                            <div class="viz-partition-head">
                              <span class="viz-part-name">
                                📑 ${this.state.topic}-P${part.id}
                                ${isLeader ? `<span class="viz-badge leader">👑 LEADER</span>` : `<span class="viz-badge follower">FOLLOWER</span>`}
                              </span>
                              <span class="viz-isr-info">
                                ISR: [${part.isr.join(',')}] ${isInISR ? '✓' : '⚠️ Out-of-sync'}
                              </span>
                            </div>

                            <!-- Log Records Track Container -->
                            <div class="viz-log-track-container">
                              <div class="viz-log-track" title="Topic Log (Left=Oldest, Right=LEO)">
                                ${part.records.length === 0 ? `<span class="viz-log-empty">Empty Log</span>` : part.records.map(rec => `
                                  <div class="viz-log-record ${rec.status}" id="rec-${part.id}-${rec.offset}" title="Offset: ${rec.offset}&#10;Key: ${rec.key}&#10;Value: ${rec.val}&#10;Status: ${rec.status}">
                                    <span class="offset-num">#${rec.offset}</span>
                                    <span class="key-preview">${escapeHtml(rec.key)}</span>
                                  </div>
                                `).join('')}
                              </div>

                              <div class="viz-log-markers">
                                <span>HW: <strong style="color: #10b981;">${part.hw}</strong></span>
                                <span>LEO: <strong style="color: #6366f1;">${part.leo}</strong></span>
                              </div>
                            </div>
                          </div>
                        `;
                      }).join('')}
                    </div>
                  `).join('')}
                </div>
              </div>
            </div>

            <!-- Right Column: Consumer Groups -->
            <div class="viz-column">
              <div class="viz-col-header">
                <span class="viz-col-title">📥 ${isVi ? 'Consumer Groups (Đọc tin)' : 'Consumer Groups'}</span>
              </div>

              ${this.state.consumerGroups.map(group => `
                <div class="viz-consumer-group-card">
                  <div class="viz-card-head">
                    <div>
                      <div class="viz-card-title">👥 ${group.id}</div>
                      <span style="font-size: 0.72rem; color: var(--text-muted);">Strategy: <strong>${group.strategy}</strong></span>
                    </div>
                    <button class="viz-btn primary" style="padding: 0.2rem 0.5rem; font-size: 0.75rem;" onclick="KafkaViz.addConsumer('${group.id}')">
                      + ${isVi ? 'Thêm Consumer' : 'Add Consumer'}
                    </button>
                  </div>

                  ${group.consumers.map(c => {
                    let consumerLag = 0;
                    c.assigned.forEach(pId => {
                      const p = this.state.partitions[pId];
                      if (p) {
                        const cur = c.offsets[pId] || 0;
                        consumerLag += Math.max(0, p.hw - cur);
                      }
                    });

                    return `
                      <div class="viz-consumer-item ${c.online ? '' : 'crashed'}" id="consumer-${c.id}">
                        <div class="viz-consumer-head">
                          <span style="font-weight: 700; font-size: 0.88rem;">👤 ${c.id}</span>
                          <div style="display: flex; gap: 0.35rem; align-items: center;">
                            <span class="viz-badge ${c.online ? 'online' : 'offline'}">${c.online ? 'ACTIVE' : 'OFFLINE'}</span>
                            <button class="viz-btn ${c.online ? 'danger' : 'success'}" style="padding: 0.1rem 0.4rem; font-size: 0.7rem;" onclick="KafkaViz.toggleConsumer('${group.id}', '${c.id}')">
                              ${c.online ? (isVi ? 'Tắt' : 'Kill') : (isVi ? 'Bật' : 'Join')}
                            </button>
                          </div>
                        </div>

                        <div class="viz-form-row">
                          <label>${isVi ? 'Phân vùng gán (Assigned)' : 'Assigned Partitions'}:</label>
                          <div class="viz-assigned-tags">
                            ${c.assigned.length === 0 ? `<span style="font-size: 0.75rem; color: var(--text-muted); font-style: italic;">None (Idle)</span>` : c.assigned.map(pId => `
                              <span class="viz-assigned-tag">P${pId} (Offset: ${c.offsets[pId] || 0})</span>
                            `).join('')}
                          </div>
                        </div>

                        <div class="viz-consumer-stats">
                          <span class="viz-lag-badge ${consumerLag === 0 ? 'zero' : 'has-lag'}">
                            Lag: ${consumerLag} msgs
                          </span>
                          <button class="viz-btn success" style="padding: 0.2rem 0.6rem; font-size: 0.75rem;" onclick="KafkaViz.executeFullMessageCycle('P1')" ${!c.online || c.assigned.length === 0 ? 'disabled' : ''}>
                            📥 ${isVi ? 'Đọc (Animate)' : 'Poll (Animate)'}
                          </button>
                        </div>
                      </div>
                    `;
                  }).join('')}
                </div>
              `).join('')}
            </div>
          </div>

          <!-- Bottom: Live Protocol Chronicle Terminal -->
          <div class="viz-chronicle-card">
            <div class="viz-chronicle-header">
              <span class="viz-chronicle-title">
                <span>💻</span> ${isVi ? 'Nhật Ký Giao Thức Kafka (Live RPC & Event Chronicle)' : 'Kafka Protocol & Cluster Chronicle'}
              </span>
              <div class="viz-chronicle-actions">
                <button class="viz-terminal-btn" onclick="KafkaViz.clearLogs()">${isVi ? 'Xóa log' : 'Clear Log'}</button>
                <button class="viz-terminal-btn" onclick="KafkaViz.copyLogs()">${isVi ? 'Sao chép' : 'Copy Logs'}</button>
              </div>
            </div>
            <div id="vizTerminalLogs" class="viz-terminal-logs">
              ${this.renderLogsHtml()}
            </div>
          </div>
        </div>
      `;

      this.bindDynamicEvents();
      setTimeout(() => this.drawConnectionCurves(), 80);
    },

    bindEvents() {
      window.addEventListener('resize', () => {
        if (document.getElementById('vizSvgLayer')) {
          this.drawConnectionCurves();
        }
      });
    },

    bindDynamicEvents() {
      const scSelect = document.getElementById('vizScenarioSelect');
      if (scSelect) {
        scSelect.addEventListener('change', (e) => {
          this.applyScenario(e.target.value);
        });
      }

      const streamBtn = document.getElementById('vizStreamToggleBtn');
      if (streamBtn) {
        streamBtn.addEventListener('click', () => this.toggleStream());
      }

      const stepBtn = document.getElementById('vizStepBtn');
      if (stepBtn) {
        stepBtn.addEventListener('click', () => this.stepForward());
      }

      const resetBtn = document.getElementById('vizResetBtn');
      if (resetBtn) {
        resetBtn.addEventListener('click', () => {
          this.resetCluster();
          this.render();
        });
      }

      const speedSelect = document.getElementById('vizSpeedSelect');
      if (speedSelect) {
        speedSelect.addEventListener('change', (e) => {
          this.state.speed = parseFloat(e.target.value) || 1;
          if (this.state.isRunningStream) {
            this.toggleStream();
            this.toggleStream();
          }
        });
      }
    },

    handleProduceClick(producerId) {
      const keyInput = document.getElementById(`pKey-${producerId}`);
      const valInput = document.getElementById(`pVal-${producerId}`);
      const partSelect = document.getElementById(`pPart-${producerId}`);
      const acksSelect = document.getElementById(`pAcks-${producerId}`);

      const producer = this.state.producers.find(p => p.id === producerId);
      if (producer) {
        if (keyInput) producer.key = keyInput.value.trim() || 'user_101';
        if (valInput) producer.val = valInput.value.trim() || '{"data":1}';
        if (partSelect) producer.partitionTarget = partSelect.value;
        if (acksSelect) producer.acks = acksSelect.value;
      }

      this.executeFullMessageCycle(producerId);
    },

    highlightNode(elementId) {
      const el = document.getElementById(elementId);
      if (el) {
        if (window.anime) {
          window.anime({
            targets: el,
            boxShadow: ['0 0 0 rgba(99,102,241,0)', '0 0 24px rgba(99,102,241,0.7)', '0 0 0 rgba(99,102,241,0)'],
            duration: 700 / this.state.speed,
            easing: 'easeInOutQuad'
          });
        } else {
          el.classList.add('highlight-write');
          setTimeout(() => el.classList.remove('highlight-write'), 800);
        }
      }
    },

    triggerFlashMessage(msg, type = 'info') {
      const banner = document.createElement('div');
      banner.style.position = 'fixed';
      banner.style.bottom = '24px';
      banner.style.right = '24px';
      banner.style.background = type === 'error' ? '#ef4444' : '#10b981';
      banner.style.color = '#ffffff';
      banner.style.padding = '0.75rem 1.25rem';
      banner.style.borderRadius = '8px';
      banner.style.fontWeight = '700';
      banner.style.fontSize = '0.9rem';
      banner.style.boxShadow = '0 6px 20px rgba(0,0,0,0.3)';
      banner.style.zIndex = '99999';
      banner.style.opacity = '0';
      banner.innerText = msg;
      document.body.appendChild(banner);

      if (window.anime) {
        window.anime({
          targets: banner,
          opacity: [0, 1],
          translateY: [20, 0],
          duration: 350,
          easing: 'easeOutCubic'
        });
      } else {
        banner.style.opacity = '1';
      }

      setTimeout(() => {
        if (window.anime) {
          window.anime({
            targets: banner,
            opacity: [1, 0],
            translateY: [0, 20],
            duration: 300,
            easing: 'easeInCubic',
            complete: () => banner.remove()
          });
        } else {
          banner.remove();
        }
      }, 2800);
    },

    clearLogs() {
      this.state.logs = [];
      const logContainer = document.getElementById('vizTerminalLogs');
      if (logContainer) logContainer.innerHTML = this.renderLogsHtml();
    },

    copyLogs() {
      const text = this.state.logs.map(l => `[${l.time}] [${l.source}] ${l.msg}`).join('\n');
      navigator.clipboard.writeText(text).then(() => {
        this.triggerFlashMessage('Logs copied to clipboard!');
      });
    },

    sleep(ms) {
      return new Promise(resolve => setTimeout(resolve, ms));
    }
  };

  function escapeHtml(str) {
    if (!str) return '';
    return String(str)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;');
  }

  window.KafkaViz = KafkaViz;
})(window);
