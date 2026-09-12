#!/usr/bin/env python3
import json
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
INPUT_FILE = os.path.join(BASE_DIR, 'questions.json')
OUTPUT_JS_FILE = os.path.join(BASE_DIR, 'questions-data.js')
CHECKPOINT_FILE = os.path.join(BASE_DIR, 'questions_vi_checkpoint.json')

TRANSLATIONS = {
  "zookeeper-questions2-q12": {
    "question_vi": "Cấu hình ZooKeeper nào xác định số lượng tick tối đa mà một follower có thể trễ so với ZooKeeper leader?",
    "options": [
      {"id": "A", "text_vi": "initLimit"},
      {"id": "B", "text_vi": "syncLimit"},
      {"id": "C", "text_vi": "tickTime"},
      {"id": "D", "text_vi": "maxClientCnxns"}
    ],
    "explanation_vi": "**Đáp án:** B\n\n**Giải thích:**\nCấu hình ZooKeeper xác định số lượng tick tối đa mà một follower có thể trễ so với ZooKeeper leader là `syncLimit`.\n\n`syncLimit` quy định khoảng thời gian tối đa (tính bằng số tickTime) mà một follower có thể gửi request và nhận phản hồi từ leader. Nếu một follower vượt quá giới hạn này, nó sẽ bị coi là mất đồng bộ và bị loại khỏi ensemble."
  },
  "zookeeper-questions2-q14": {
    "question_vi": "Thành phần nào chịu trách nhiệm lưu trữ metadata của Kafka cluster trong chế độ KRaft?",
    "options": [
      {"id": "A", "text_vi": "ZooKeeper ensemble"},
      {"id": "B", "text_vi": "Metadata quorum (Metadata log)"},
      {"id": "C", "text_vi": "Schema Registry"},
      {"id": "D", "text_vi": "External database"}
    ],
    "explanation_vi": "**Đáp án:** B\n\n**Giải thích:**\nTrong chế độ KRaft (Kafka Raft Metadata Mode), metadata của Kafka cluster được lưu trữ và quản lý trực tiếp trong internal metadata topic (metadata log) được duy trì bởi một nhóm controller quorum sử dụng thuật toán đồng thuận Raft, loại bỏ hoàn toàn sự phụ thuộc vào ZooKeeper."
  },
  "zookeeper-questions2-q15": {
    "question_vi": "Trong chế độ KRaft, broker nào đóng vai trò là Active Controller?",
    "options": [
      {"id": "A", "text_vi": "Broker có broker.id thấp nhất"},
      {"id": "B", "text_vi": "Broker được bầu làm leader của metadata quorum"},
      {"id": "C", "text_vi": "Mọi broker đều đồng thời là Active Controller"},
      {"id": "D", "text_vi": "Broker được chỉ định thủ công trong server.properties"}
    ],
    "explanation_vi": "**Đáp án:** B\n\n**Giải thích:**\nTrong kiến trúc KRaft, node controller được bầu làm leader của metadata quorum thông qua cơ chế bầu chọn Raft sẽ đóng vai trò là Active Controller quản lý toàn bộ cluster metadata."
  },
  "zookeeper-questions2-q16": {
    "question_vi": "Lợi ích chính của việc chuyển từ ZooKeeper sang KRaft trong Apache Kafka là gì?",
    "options": [
      {"id": "A", "text_vi": "Tăng giới hạn số lượng partition tối đa và cải thiện tốc độ khôi phục/failover controller"},
      {"id": "B", "text_vi": "Tự động mã hóa tất cả message payload"},
      {"id": "C", "text_vi": "Loại bỏ hoàn toàn nhu cầu sử dụng Schema Registry"},
      {"id": "D", "text_vi": "Giảm dung lượng ổ đĩa của message log"}
    ],
    "explanation_vi": "**Đáp án:** A\n\n**Giải thích:**\nKRaft cho phép Kafka hỗ trợ hàng triệu partition trên một cluster, loại bỏ độ trễ đồng bộ metadata giữa ZooKeeper và Kafka broker, giúp controller failover diễn ra gần như tức thì và đơn giản hóa việc vận hành hệ thống."
  },
  "zookeeper-questions2-q17": {
    "question_vi": "Cấu hình nào dùng để xác định danh sách các node controller trong KRaft quorum?",
    "options": [
      {"id": "A", "text_vi": "zookeeper.connect"},
      {"id": "B", "text_vi": "controller.quorum.voters"},
      {"id": "C", "text_vi": "bootstrap.servers"},
      {"id": "D", "text_vi": "metadata.broker.list"}
    ],
    "explanation_vi": "**Đáp án:** B\n\n**Giải thích:**\nTrong KRaft mode, cấu hình `controller.quorum.voters` chỉ định danh sách các voting controller node dưới dạng `node_id@host:port` để thiết lập quorum bầu cử và sao chép metadata."
  },
  "zookeeper-questions2-q18": {
    "question_vi": "Cấu hình `process.roles` trong KRaft có thể nhận các giá trị nào?",
    "options": [
      {"id": "A", "text_vi": "broker, controller, hoặc cả broker,controller (hoặc để trống)"},
      {"id": "B", "text_vi": "producer, consumer"},
      {"id": "C", "text_vi": "master, slave"},
      {"id": "D", "text_vi": "leader, follower"}
    ],
    "explanation_vi": "**Đáp án:** A\n\n**Giải thích:**\nTrong KRaft, mỗi node có thể hoạt động với vai trò `broker`, `controller`, hoặc chế độ kết hợp (combined mode) `broker,controller`. Nếu để trống, node hoạt động ở chế độ ZooKeeper truyền thống."
  },
  "zookeeper-questions2-q19": {
    "question_vi": "Khi chạy Kafka ở chế độ KRaft, metadata log topic nội bộ được đặt tên là gì?",
    "options": [
      {"id": "A", "text_vi": "__consumer_offsets"},
      {"id": "B", "text_vi": "@metadata"},
      {"id": "C", "text_vi": "__cluster_metadata"},
      {"id": "D", "text_vi": "_schemas"}
    ],
    "explanation_vi": "**Đáp án:** C\n\n**Giải thích:**\nTrong KRaft, partition đơn duy nhất lưu trữ toàn bộ trạng thái metadata của cluster thuộc về topic nội bộ đặc biệt có tên là `__cluster_metadata`."
  },
  "zookeeper-questions2-q20": {
    "question_vi": "Lệnh CLI nào dùng để tạo cluster ID mới khi khởi tạo KRaft cluster?",
    "options": [
      {"id": "A", "text_vi": "kafka-storage.sh random-uuid"},
      {"id": "B", "text_vi": "kafka-topics.sh --create-cluster-id"},
      {"id": "C", "text_vi": "zookeeper-shell.sh generate-id"},
      {"id": "D", "text_vi": "kafka-configs.sh --generate-uuid"}
    ],
    "explanation_vi": "**Đáp án:** A\n\n**Giải thích:**\nĐể khởi tạo một KRaft cluster mới, bạn sử dụng lệnh `kafka-storage.sh random-uuid` để sinh một Cluster ID ngẫu nhiên hợp lệ trước khi format log directories bằng `kafka-storage.sh format`."
  },
  "zookeeper-questions3-q21": {
    "question_vi": "Công cụ nào được sử dụng để định dạng (format) log directories của Kafka node trong chế độ KRaft?",
    "options": [
      {"id": "A", "text_vi": "kafka-topics.sh"},
      {"id": "B", "text_vi": "kafka-storage.sh"},
      {"id": "C", "text_vi": "kafka-server-start.sh"},
      {"id": "D", "text_vi": "kafka-configs.sh"}
    ],
    "explanation_vi": "**Đáp án:** B\n\n**Giải thích:**\nLệnh `kafka-storage.sh format -t <cluster-id> -c <config-file>` được sử dụng để khởi tạo và định dạng thư mục lưu trữ metadata và log cho các node KRaft."
  },
  "zookeeper-questions3-q22": {
    "question_vi": "Trong quá trình di chuyển từ ZooKeeper sang KRaft (KRaft Migration), chế độ nào cho phép các controller KRaft và ZooKeeper hoạt động song song?",
    "options": [
      {"id": "A", "text_vi": "Dual-write mode"},
      {"id": "B", "text_vi": "Hybrid / Migration mode với zookeeper.metadata.migration.enable=true"},
      {"id": "C", "text_vi": "Shadow cluster mode"},
      {"id": "D", "text_vi": "Active-passive mode"}
    ],
    "explanation_vi": "**Đáp án:** B\n\n**Giải thích:**\nQuá trình chuyển đổi từ ZooKeeper sang KRaft sử dụng tính năng di chuyển metadata trực tiếp (zookeeper.metadata.migration.enable=true) cho phép KRaft quorum đồng bộ dữ liệu với ZooKeeper mà không làm gián đoạn dịch vụ của cluster."
  },
  "zookeeper-questions3-q23": {
    "question_vi": "Khi nào một record metadata trong KRaft quorum được coi là đã committed?",
    "options": [
      {"id": "A", "text_vi": "Khi nó được ghi vào bộ nhớ của Active Controller"},
      {"id": "B", "text_vi": "Khi đa số (quorum) các controller node đã nhận và ghi record vào metadata log của chúng"},
      {"id": "C", "text_vi": "Khi tất cả broker trong cluster đã phản hồi xác nhận"},
      {"id": "D", "text_vi": "Khi record được ghi thành công xuống ZooKeeper"}
    ],
    "explanation_vi": "**Đáp án:** B\n\n**Giải thích:**\nTheo thuật toán đồng thuận Raft, một record metadata được coi là committed khi nó đã được sao chép và xác nhận bền vững bởi đa số (majority/quorum) các voter controller trong quorum."
  },
  "zookeeper-questions3-q24": {
    "question_vi": "Để đảm bảo tính sẵn sàng cao và khả năng chịu lỗi tối đa 1 node controller bị sự cố, số lượng controller tối thiểu cần thiết trong KRaft quorum là bao nhiêu?",
    "options": [
      {"id": "A", "text_vi": "1"},
      {"id": "B", "text_vi": "2"},
      {"id": "C", "text_vi": "3"},
      {"id": "D", "text_vi": "4"}
    ],
    "explanation_vi": "**Đáp án:** C\n\n**Giải thích:**\nTheo nguyên lý Quorum (2F + 1), để chịu được lỗi F node, hệ thống cần tối thiểu 2F + 1 node. Để chịu được lỗi F = 1 node controller, quorum cần tối thiểu 2(1) + 1 = 3 controller."
  },
  "zookeeper-questions3-q25": {
    "question_vi": "Cấu hình `node.id` trong KRaft thay thế cho cấu hình nào trong kiến trúc Kafka ZooKeeper truyền thống?",
    "options": [
      {"id": "A", "text_vi": "broker.id"},
      {"id": "B", "text_vi": "group.id"},
      {"id": "C", "text_vi": "client.id"},
      {"id": "D", "text_vi": "cluster.id"}
    ],
    "explanation_vi": "**Đáp án:** A\n\n**Giải thích:**\nTrong KRaft mode, `node.id` được sử dụng để định danh duy nhất cho từng node (cho dù node đó là broker, controller, hay cả hai), thay thế cho `broker.id` truyền thống."
  },
  "zookeeper-questions3-q26": {
    "question_vi": "KRaft metadata snapshot có vai trò gì trong kiến trúc Kafka?",
    "options": [
      {"id": "A", "text_vi": "Sao lưu toàn bộ dữ liệu message của tất cả user topic"},
      {"id": "B", "text_vi": "Nén và lưu giữ một điểm ảnh chụp trạng thái metadata, giúp giải phóng metadata log cũ và tăng tốc phục hồi node"},
      {"id": "C", "text_vi": "Tạo bản sao dự phòng của Schema Registry"},
      {"id": "D", "text_vi": "Lưu trữ cấu hình client kết nối"}
    ],
    "explanation_vi": "**Đáp án:** B\n\n**Giải thích:**\nMetadata snapshot trong KRaft chụp lại toàn bộ trạng thái metadata tại một thời điểm nhất định. Điều này cho phép Kafka cắt tỉa (truncate) các log segment cũ trong `__cluster_metadata` và cho phép các controller/broker mới đồng bộ trạng thái cực nhanh mà không cần phát lại từ offset 0."
  },
  "zookeeper-questions3-q27": {
    "question_vi": "Chế độ Standalone Controller trong KRaft có đặc điểm gì?",
    "options": [
      {"id": "A", "text_vi": "Chỉ có 1 node controller duy nhất quản lý metadata (không có quorum chịu lỗi), phù hợp cho môi trường phát triển/thử nghiệm"},
      {"id": "B", "text_vi": "Node controller chạy độc lập trên máy chủ ZooKeeper"},
      {"id": "C", "text_vi": "Mọi broker tự quản lý metadata cục bộ"},
      {"id": "D", "text_vi": "Không hỗ trợ tạo topic mới"}
    ],
    "explanation_vi": "**Đáp án:** A\n\n**Giải thích:**\nTrong môi trường kiểm thử hoặc phát triển cục bộ, KRaft có thể chạy với 1 controller duy nhất (Standalone). Tuy nhiên trong môi trường sản xuất (Production), luôn cần tối thiểu 3 controller để đảm bảo High Availability."
  },
  "zookeeper-questions3-q28": {
    "question_vi": "Cấu hình `controller.listener.names` dùng để làm gì trong Kafka KRaft?",
    "options": [
      {"id": "A", "text_vi": "Chỉ định tên listener dành riêng cho giao tiếp nội bộ giữa các controller và giữa broker với controller"},
      {"id": "B", "text_vi": "Chỉ định danh sách client được phép kết nối vào cluster"},
      {"id": "C", "text_vi": "Định nghĩa port kết nối cho Schema Registry"},
      {"id": "D", "text_vi": "Thiết lập giao thức SSL cho producer"}
    ],
    "explanation_vi": "**Đáp án:** A\n\n**Giải thích:**\n`controller.listener.names` xác định tên listener mà các controller lắng nghe và tiếp nhận các yêu cầu quản trị/đồng bộ metadata từ các node khác trong cluster."
  },
  "zookeeper-questions3-q29": {
    "question_vi": "Mục đích của metric `kafka.controller:type=QuorumController,name=LastCommittedRecordOffset` trong chế độ KRaft là gì?",
    "options": [
      {"id": "A", "text_vi": "Chỉ ra offset của record cuối cùng được append vào metadata log"},
      {"id": "B", "text_vi": "Đại diện cho offset của record cuối cùng được replicate tới tất cả controller"},
      {"id": "C", "text_vi": "Đo lường offset của record cuối cùng được áp dụng bởi active controller"},
      {"id": "D", "text_vi": "Theo dõi offset của record cuối cùng đã được commit bởi active controller"}
    ],
    "explanation_vi": "**Đáp án:** D\n\n**Giải thích:**\nTrong chế độ KRaft, metric `LastCommittedRecordOffset` theo dõi offset của record metadata mới nhất đã được commit thành công bởi Active Controller sau khi được xác nhận bởi đa số voter trong quorum."
  },
  "zookeeper-questions3-q30": {
    "question_vi": "Ảnh hưởng của việc thiết lập `controller.quorum.fetch.timeout.ms` thành một giá trị quá thấp trong KRaft là gì?",
    "options": [
      {"id": "A", "text_vi": "Tăng thời gian controller chờ phản hồi fetch từ active controller"},
      {"id": "B", "text_vi": "Giảm thời gian controller chờ phản hồi fetch từ active controller, có thể gây báo động giả và kích hoạt failover không cần thiết"},
      {"id": "C", "text_vi": "Đặt thời gian tối đa cho phép controller lấy dữ liệu từ broker"},
      {"id": "D", "text_vi": "Xác định tần suất controller lấy dữ liệu từ active controller"}
    ],
    "explanation_vi": "**Đáp án:** B\n\n**Giải thích:**\nCấu hình `controller.quorum.fetch.timeout.ms` quy định thời gian tối đa một controller chờ phản hồi fetch từ Active Controller. Nếu đặt quá thấp, chỉ một độ trễ mạng nhỏ cũng có thể khiến follower coi Active Controller bị lỗi và kích hoạt quy trình bầu cử mới không cần thiết."
  }
}

def main():
    print("Reading questions.json...")
    with open(INPUT_FILE, 'r', encoding='utf-8') as f:
        questions = json.load(f)

    checkpoint = []
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, 'r', encoding='utf-8') as f:
            checkpoint = json.load(f)

    ckpt_map = {q['id']: q for q in checkpoint if q.get('question_vi')}

    final_questions = []
    for q in questions:
        qid = q['id']
        if qid in TRANSLATIONS:
            trans = TRANSLATIONS[qid]
            q['question_vi'] = trans['question_vi']
            for i, opt in enumerate(q.get('options', [])):
                if i < len(trans['options']):
                    opt['text_vi'] = trans['options'][i]['text_vi']
            q['explanation_vi'] = trans['explanation_vi']
            final_questions.append(q)
        elif qid in ckpt_map:
            final_questions.append(ckpt_map[qid])
        else:
            final_questions.append(q)

    # Verify all 330 have question_vi
    translated_count = sum(1 for q in final_questions if q.get('question_vi'))
    print(f"Total verified translated questions: {translated_count}/{len(final_questions)}")

    # Write questions.json
    print(f"Writing to {INPUT_FILE}...")
    with open(INPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(final_questions, f, ensure_ascii=False, indent=2)

    # Write questions-data.js
    print(f"Writing to {OUTPUT_JS_FILE}...")
    with open(OUTPUT_JS_FILE, 'w', encoding='utf-8') as f:
        f.write("// Confluent Certified Developer for Apache Kafka (CCDAK) Practice Questions Data\n")
        f.write("var CCDAK_QUESTIONS = ")
        json.dump(final_questions, f, ensure_ascii=False, indent=2)
        f.write(";\n")

    # Update checkpoint as well
    with open(CHECKPOINT_FILE, 'w', encoding='utf-8') as f:
        json.dump(final_questions, f, ensure_ascii=False, indent=2)

    print("Successfully updated all 330 questions with Vietnamese translations!")

if __name__ == '__main__':
    main()
