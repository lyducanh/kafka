---
id: kraft-vs-zookeeper-architecture
title: Kiến trúc KRaft vs ZooKeeper trong Apache Kafka: Sự đột phá của KIP-500
title_en: KRaft vs ZooKeeper in Apache Kafka: Understanding the KIP-500 Revolution
date: 2026-09-12
author: Cloud Infrastructure Lead
tags: [KRaft, ZooKeeper, Architecture, KIP-500, Metadata]
summary: So sánh kiến trúc metadata truyền thống dựa trên ZooKeeper và cơ chế đồng thuận Raft nội bộ (KRaft), lợi ích về khả năng scale và thời gian phục hồi.
summary_en: A deep architectural comparison between traditional ZooKeeper metadata management and Kafka's built-in Raft consensus (KRaft).
cover_icon: 🚀
reading_time: 6 min
---

# Kiến trúc KRaft vs ZooKeeper trong Apache Kafka: Sự đột phá của KIP-500

Trong hơn một thập kỷ, Apache Kafka dựa vào **Apache ZooKeeper** để quản lý metadata, bầu chọn controller và theo dõi trạng thái cluster. Tuy nhiên, sự xuất hiện của **KIP-500** và chế độ **KRaft (Kafka Raft Metadata mode)** đã đánh dấu bước chuyển mình quan trọng nhất trong lịch sử phát triển của Kafka.

---

## 1. Hạn chế của kiến trúc ZooKeeper truyền thống

Trong mô hình ZooKeeper:
1. **Metadata bị phân tán (Split Metadata)**: Metadata vừa nằm trong ZooKeeper vừa được cache trong bộ nhớ của Active Controller.
2. **Đồng bộ chậm khi Failover**: Khi Active Controller gặp sự cố, Controller mới phải đọc lại toàn bộ trạng thái từ ZooKeeper, mất từ vài chục giây đến vài phút nếu cluster có hàng chục nghìn partition.
3. **Giới hạn quy mô (Partition Limits)**: Cụm cluster thường bị giới hạn tối đa khoảng 200,000 partition.

```
[Brokers] <---> [Active Controller] <---> [ZooKeeper Ensemble]
```

---

## 2. Kiến trúc KRaft (Kafka Raft Metadata Mode)

Với KRaft, ZooKeeper được loại bỏ hoàn toàn. Metadata của cluster được lưu trữ như một topic nội bộ chuyên dụng (`__cluster_metadata`) và được đồng thuận thông qua thuật toán **Raft**:

```
[Brokers] <==== Quorum RPC ====> [KRaft Controller Quorum (Leader & Followers)]
```

### Ưu điểm vượt trội của KRaft:
- **Thời gian phục hồi Controller tức thì**: Tất cả các controller follower trong Quorum luôn giữ trạng thái metadata cập nhật trong bộ nhớ. Khi Leader lỗi, Leader mới sẵn sàng phục vụ gần như ngay lập tức (< 1 giây).
- **Khả năng mở rộng quy mô cực lớn**: Hỗ trợ tới hàng triệu partition trên một cluster duy nhất.
- **Đơn giản hóa vận hành**: Chỉ cần quản lý và giám sát một hệ thống phần mềm duy nhất (Apache Kafka) thay vì hai cụm độc lập.

---

## 3. Bảng so sánh tổng hợp

| Tiêu chí | ZooKeeper Mode | KRaft Mode |
|---|---|---|
| **Lưu trữ Metadata** | Hệ thống file phân tán ZooKeeper | Topic nội bộ `__cluster_metadata` |
| **Quản lý Định danh** | `broker.id` | `node.id` |
| **Vai trò Node** | Broker thuần túy | `process.roles=broker,controller` |
| **Cấu hình Quorum** | `zookeeper.connect` | `controller.quorum.voters` |
| **Thời gian Controller Failover** | Vài chục giây đến vài phút | Dưới 1 giây (Instantaneous) |
| **Khởi tạo Cluster** | Tự động khi kết nối ZK | Cần định dạng bằng `kafka-storage.sh format` |
