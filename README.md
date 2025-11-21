# Kafka Assignment - Order Processing System

This project demonstrates a Kafka-based real-time order processing pipeline using Python.

## 📌 Features
- Produces and consumes order messages to/from Kafka
- Avro schema-based serialization
- Retry logic for temporary failures
- Dead Letter Queue (DLQ) for invalid orders
- Tracks running average price of successfully processed orders

## 🛠️ Tech Stack
- **Kafka** (Local server)
- **Python**
- **confluent-kafka** (Kafka client)
- **Avro** for message schema

## 📂 Folder Structure
```
Big Data/Assignment 3/
│
├── producer.py
├── consumer.py
├── order.avsc
├── README.md
└── kafka_env/ (virtual environment)
```

## 📝 Avro Schema
`order.avsc` defines fields in an order message:
```json
{
  "type": "record",
  "name": "Order",
  "fields": [
    { "name": "orderId", "type": "string" },
    { "name": "product", "type": "string" },
    { "name": "price", "type": "float" }
  ]
}
```

## ▶ How to Run

### 1️⃣ Activate Virtual Environment
```bash
kafka_env\Scripts\activate
```

### 2️⃣ Install Dependencies
```bash
pip install confluent-kafka avro-python3
```

### 3️⃣ Start Kafka
Make sure Kafka server is running:
```bash
bin\windows\kafka-server-start.bat config\server.properties
```

### 4️⃣ Create Topics
```bash
kafka-topics --create --topic orders --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
kafka-topics --create --topic orders-retry --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
kafka-topics --create --topic orders-dlq --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

### 5️⃣ Run Producer
```bash
python producer.py
```

### 6️⃣ Run Consumer
```bash
python consumer.py
```

## 🔁 Retry & Error Handling
| Condition | Action |
|----------|--------|
| Temporary error | Resent to `orders-retry` (Max 3 retries) |
| Invalid order | Sent to `orders-dlq` |

## 📊 Output Example
```
✓ Processed: Order O1001 → Keyboard → $45.50
Running Average: $45.50
⚠ Retrying (1/3)
✗ Sending to DLQ — Invalid order data
```


