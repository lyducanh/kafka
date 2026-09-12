---
id: kafka-streams-kstream-vs-ktable
title: Hiểu sâu Kafka Streams: Phân biệt KStream, KTable và GlobalKTable
title_en: Deep Dive into Kafka Streams: KStream vs KTable vs GlobalKTable
date: 2026-09-12
author: Distributed Systems Engineer
tags: [Kafka Streams, KTable, KStream, Stream Processing]
summary: Phân tích tường tận sự đối ngẫu Stream-Table (Duality), cách hoạt động của State Store RocksDB và kịch bản áp dụng KStream vs KTable trong thực tế.
summary_en: A deep architectural breakdown of the Stream-Table Duality, RocksDB state stores, and practical join scenarios in Kafka Streams.
cover_icon: ⚡
reading_time: 6 min
---

# Hiểu sâu Kafka Streams: Phân biệt KStream, KTable và GlobalKTable

Trong hệ sinh thái Apache Kafka, **Kafka Streams** là thư viện client Java tiêu chuẩn giúp xử lý luồng dữ liệu thời gian thực (stream processing) mà không cần triển khai cụm cluster tính toán cồng kềnh như Flink hay Spark.

Khái niệm cốt lõi quan trọng nhất trong Kafka Streams là **Stream-Table Duality (Tính đối ngẫu giữa Luồng và Bảng)**.

---

## 1. KStream (Event Stream)

`KStream` đại diện cho một luồng các sự kiện (Record Stream). Mỗi record mới được đưa vào luồng là một **sự kiện độc lập** bổ sung (Insert / Append-only).

```java
// Khởi tạo KStream từ topic "user-clicks"
KStream<String, String> clickStream = builder.stream("user-clicks");

// Thao tác Stateless: Lọc và biến đổi dữ liệu
KStream<String, String> searchClicks = clickStream
    .filter((key, value) -> value.contains("search"))
    .mapValues(value -> value.toUpperCase());
```

- **Đặc điểm:** Không ghi đè bản ghi cũ; nếu gửi 3 record cùng key `"user1"`, KStream vẫn chứa cả 3 sự kiện riêng biệt.

---

## 2. KTable (Changelog Stream / Update Table)

`KTable` đại diện cho một bảng dữ liệu trạng thái (State Table). Mỗi record mới có cùng `Key` sẽ hoạt động như một câu lệnh **UPSERT (Update or Insert)**.

- Nếu record có `Key = "user1", Value = "VIP"` -> Thêm/Cập nhật user1 là VIP.
- Nếu gửi tiếp `Key = "user1", Value = "Platinum"` -> Trạng thái của user1 được cập nhật thành Platinum.
- Nếu gửi record `Key = "user1", Value = null` (**Tombstone**) -> Xóa bản ghi user1 khỏi bảng.

```java
// Khởi tạo KTable từ topic "user-profiles"
KTable<String, String> userProfiles = builder.table("user-profiles");
```

---

## 3. So sánh tổng hợp: KStream vs KTable vs GlobalKTable

| Tiêu chí | KStream | KTable | GlobalKTable |
|---|---|---|---|
| **Ngữ nghĩa dữ liệu** | Insert / Append (Fact Stream) | Upsert / Delete (Dimension/State) | Upsert cho toàn bộ dữ liệu toàn cục |
| **Phân vùng (Partitioning)** | Phân vùng theo task | Phân vùng theo task (Co-partitioned) | Toàn bộ dữ liệu được replicate về mỗi instance |
| **Tombstone (`value=null`)** | Xử lý như một message bình thường | Xóa key tương ứng khỏi state store | Xóa key tương ứng khỏi state store |
| **Yêu cầu Co-partitioning khi Join**| Bắt buộc cùng số partition và key | Bắt buộc cùng số partition và key | **Không cần** co-partitioning |

---

## 4. Bảng Tra Cứu Phép Join trong Kafka Streams

| Phép Join (Left ⋈ Right) | Hỗ trợ Windowing? | Có yêu cầu Co-partitioning? |
|---|---|---|
| **KStream ⋈ KStream** | Bắt buộc (Windowed Join) | Có (Cùng số partition & partitioning strategy) |
| **KStream ⋈ KTable** | Không windowed | Có |
| **KStream ⋈ GlobalKTable**| Không windowed | **Không yêu cầu** (rất tiện lợi khi join bảng tĩnh) |
| **KTable ⋈ KTable** | Không windowed | Có |

> [!TIP]
> **Mẹo thi CCDAK**: Phép join giữa 2 `KStream` bắt buộc phải có cấu hình Window (ví dụ `JoinWindows.ofTimeDifferenceWithNoGrace(...)`) vì luồng sự kiện là vô hạn, không thể join nếu không giới hạn khung thời gian.
