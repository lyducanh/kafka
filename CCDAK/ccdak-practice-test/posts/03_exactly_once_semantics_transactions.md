---
id: exactly-once-semantics-transactions
title: Exactly-Once Semantics (EOS) trong Kafka: Idempotence và Transactions hoạt động thế nào?
title_en: Achieving Exactly-Once Semantics in Apache Kafka: Idempotence & Transactions
date: 2026-09-12
author: Kafka Core Architect
tags: [Idempotence, Transactions, Exactly-Once, Reliability]
summary: Khám phá cơ chế Producer ID (PID), Sequence Number, Transaction Coordinator và Two-Phase Commit giúp Kafka đạt chuẩn Exactly-Once.
summary_en: Explore how Producer IDs, Sequence Numbers, Transaction Coordinators, and 2-Phase Commits enable end-to-end Exactly-Once processing in Kafka.
cover_icon: 🛡️
reading_time: 8 min
---

# Exactly-Once Semantics (EOS) trong Kafka: Idempotence và Transactions

Trong các hệ thống phân tán, việc xử lý dữ liệu với độ tin cậy tuyệt đối là bài toán khó nhất. Apache Kafka cung cấp 3 mức độ bảo đảm truyền tải (Delivery Semantics):

1. **At-least-once (Ít nhất một lần)**: Dữ liệu không bao giờ mất, nhưng có thể bị trùng lặp khi xảy ra retry.
2. **At-most-once (Tối đa một lần)**: Dữ liệu có thể bị mất, nhưng không bao giờ trùng lặp.
3. **Exactly-once (Chính xác một lần)**: Dữ liệu được ghi và xử lý chính xác một lần duy nhất, ngay cả khi broker hoặc client gặp sự cố.

---

## 1. Idempotent Producer (Chống trùng lặp trên 1 Partition)

Từ Kafka 3.0 trở đi, cấu hình `enable.idempotence=true` đã được bật **mặc định**.

### Cách hoạt động:
- Mỗi Producer khi khởi động được gán một **Producer ID (PID)** duy nhất bởi broker.
- Mỗi message gửi tới một partition kèm theo một số thứ tự tăng dần (**Sequence Number**).
- Broker ghi nhớ Sequence Number cao nhất đã nhận cho mỗi PID trên từng partition:
  - Nếu broker nhận được `Sequence Number == Current + 1`: Chấp nhận và lưu record.
  - Nếu `Sequence Number <= Current`: Broker phát hiện trùng lặp do Producer gửi lại (retry) -> Bỏ qua bản ghi nhưng vẫn trả về ACK thành công để Producer không retry nữa.

```properties
# Bật Idempotent Producer
enable.idempotence=true
acks=all
max.in.flight.requests.per.connection=5
retries=2147483647
```

---

## 2. Kafka Transactions (Ghi nguyên tử trên Nhiều Partition)

Idempotence chỉ bảo vệ trên **một partition đơn lẻ**. Để thực hiện ghi dữ liệu trên **nhiều partition / topic** hoặc kết hợp **Read-Process-Write** theo mô hình nguyên tử (All-or-Nothing), chúng ta sử dụng **Kafka Transactions**.

```java
Properties props = new Properties();
props.put("bootstrap.servers", "localhost:9092");
props.put("transactional.id", "order-processing-tx-1"); // Bắt buộc
props.put("enable.idempotence", "true");

KafkaProducer<String, String> producer = new KafkaProducer<>(props);

// 1. Khởi tạo transaction
producer.initTransactions();

try {
    // 2. Bắt đầu transaction
    producer.beginTransaction();
    
    producer.send(new ProducerRecord<>("orders", "order123", "PAID"));
    producer.send(new ProducerRecord<>("notifications", "user123", "Order confirmed"));
    
    // Gửi kèm committed offset của consumer nếu là luồng Read-Process-Write
    // producer.sendOffsetsToTransaction(offsets, consumerGroupId);
    
    // 3. Commit toàn bộ
    producer.commitTransaction();
} catch (ProducerFencedException | OutOfOrderSequenceException e) {
    producer.close();
} catch (KafkaException e) {
    // 4. Rollback nếu có lỗi
    producer.abortTransaction();
}
```

---

## 3. Transaction Isolation Level ở phía Consumer

Khi sử dụng Transactions, Consumer cần cấu hình `isolation.level`:

- `isolation.level=read_uncommitted` *(Mặc định)*: Đọc tất cả các message, bao gồm cả những transaction chưa commit hoặc đã bị abort.
- `isolation.level=read_committed`: Chỉ đọc những message thuộc về transaction **đã commit thành công** hoặc các message non-transactional. Consumer sẽ dừng lại ở **Last Stable Offset (LSO)** khi có transaction đang mở.

---

## 4. Tóm tắt nhanh cho kỳ thi CCDAK

| Khái niệm | Yêu cầu | Phạm vi bảo vệ |
|---|---|---|
| **Idempotent Producer** | `enable.idempotence=true` | Tránh trùng lặp trên 1 Partition duy nhất |
| **Transactional Producer** | `transactional.id` được cấu hình | Ghi nguyên tử trên Nhiều Topic / Partition |
| **Consumer Isolation** | `isolation.level=read_committed` | Chỉ tiêu thụ các transaction đã commit thành công |
