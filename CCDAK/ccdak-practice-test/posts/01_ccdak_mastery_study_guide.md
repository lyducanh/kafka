---
id: ccdak-mastery-study-guide
title: Bí kíp luyện thi và vượt qua chứng chỉ CCDAK (Confluent Certified Developer for Apache Kafka)
title_en: Master the CCDAK Exam: Complete Study & Preparation Guide
date: 2026-09-12
author: Apache Kafka Certified Specialist
tags: [CCDAK, Certification, Best Practices, Kafka Architecture]
summary: Hướng dẫn chi tiết lộ trình ôn luyện, phân tích trọng số các Domain thi và chiến thuật làm 60 câu trắc nghiệm CCDAK trong 90 phút.
summary_en: A comprehensive guide covering domain weightings, core architectural concepts, and practical exam tactics to pass the Confluent CCDAK certification.
cover_icon: 🎓
reading_time: 7 min
---

# Bí kíp luyện thi và vượt qua chứng chỉ CCDAK (Confluent Certified Developer)

Chứng chỉ **CCDAK (Confluent Certified Developer for Apache Kafka)** là một trong những chứng chỉ uy tín và giá trị nhất đối với các kỹ sư dữ liệu, backend developer và kiến trúc sư hệ thống làm việc với kiến trúc hướng sự kiện (Event-Driven Architecture).

---

## 1. Cấu trúc bài thi CCDAK chuẩn

| Tiêu chí | Chi tiết |
|---|---|
| **Số lượng câu hỏi** | 60 câu hỏi (Single & Multiple Choice) |
| **Thời gian làm bài** | 90 phút |
| **Điểm đỗ (Passing Score)** | Thường từ 70% - 75% trở lên |
| **Hình thức thi** | Trực tuyến có giám sát (Proctored Online Exam) |

---

## 2. Phân bổ trọng số kiến thức (Blueprint Domains)

Bài thi CCDAK tập trung vào 5 nhóm chủ đề lớn:

1. **Architecture & Broker Fundamentals (30%)**:
   - Quản lý Partition, Replication Factor, Leader/Follower replica và ISR (In-Sync Replicas).
   - Log Retention, Segment rolling (`log.segment.bytes`, `log.retention.ms`) và Log Compaction (`cleanup.policy=compact`).
   - Quorum Controllers, KRaft metadata log (`__cluster_metadata`) và cấu trúc cluster.

2. **Kafka Producer & Reliability (20%)**:
   - Bộ đệm producer (`buffer.memory`, `batch.size`, `linger.ms`, `compression.type`).
   - Mức độ bảo đảm ghi (`acks=0`, `acks=1`, `acks=all` kết hợp với `min.insync.replicas`).
   - Idempotent Producer (`enable.idempotence=true`) và Transactional API.

3. **Kafka Consumer & Group Management (20%)**:
   - Consumer Group Rebalance, partition assignment strategies (Range, RoundRobin, Sticky, CooperativeSticky).
   - Quản lý offset (`enable.auto.commit`, `commitSync()`, `commitAsync()`).
   - Cấu hình polling quan trọng: `max.poll.interval.ms`, `max.poll.records`, `session.timeout.ms`.

4. **Kafka Streams & Real-time Processing (15%)**:
   - Phân biệt sự khác nhau giữa **KStream** (luồng sự kiện append-only) và **KTable** (bảng trạng thái changelog).
   - State Stores (RocksDB), Stateless vs Stateful operations.
   - Các loại Windowing: Tumbling, Hopping, Sliding và Session windows.

5. **Schema Registry & Kafka Connect (15%)**:
   - Các chuẩn Schema: Avro, Protobuf, JSON Schema.
   - Các chế độ Schema Evolution: `BACKWARD`, `FORWARD`, `FULL`, `NONE` và biến thể `_TRANSITIVE`.
   - Cấu hình Kafka Connect, Source/Sink Connectors, SMT (Single Message Transforms) và Converters.

---

## 3. Top 5 bẫy thường gặp trong đề thi

> [!WARNING]
> **Bẫy 1: acks=all với min.insync.replicas**  
> `acks=all` (hoặc `-1`) **KHÔNG** có nghĩa là tin nhắn phải được ghi vào tất cả các replica trong cluster. Nó chỉ yêu cầu ghi thành công vào **tất cả các replica đang nằm trong ISR**, và số lượng này phải $\ge$ `min.insync.replicas`.

> [!IMPORTANT]
> **Bẫy 2: Log Compaction và Tombstone**  
> Khi một record có `key != null` và `value = null` (gọi là Tombstone), nó đánh dấu xóa record đó. Log cleaner chỉ xóa bỏ hoàn toàn tombstone sau khoảng thời gian `delete.retention.ms`.

> [!TIP]
> **Bẫy 3: Schema Evolution Rule**  
> - `BACKWARD` compatibility: Consumer với schema mới có thể đọc dữ liệu ghi bởi schema cũ (Quy tắc: Xóa trường cũ hoặc thêm trường có giá trị Default).  
> - `FORWARD` compatibility: Consumer với schema cũ có thể đọc dữ liệu ghi bởi schema mới.

---

## 4. Lời khuyên khi luyện tập trên hệ thống

1. **Làm quen với Chế độ Thi Thử (Timed Exam)**: Tập làm 60 câu trong 90 phút để căn chuẩn tốc độ 1.5 phút/câu.
2. **Sử dụng Thẻ Ghi Nhớ (Flashcards)**: Dùng chế độ Flashcard để ôn lại các định nghĩa và công thức cấu hình nhanh.
3. **Đánh dấu (Flag) câu sai**: Xem lại danh sách câu hỏi làm sai ở bộ lọc Sidebar để nắm vững bản chất giải thích.

*Chúc các bạn đạt kết quả thật cao và sớm sở hữu chứng chỉ Confluent CCDAK!*
