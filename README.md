# ⚡ Real-Time E-Commerce Streaming Data Pipeline

An end-to-end **Data Engineering project** focused on building a real-time event processing pipeline for an e-commerce platform.

The project simulates live e-commerce order events and streams them through **Apache Kafka**. As the project progresses, these events will be consumed, validated, stored, analyzed, and eventually displayed through a real-time analytics dashboard.

The main goal is to understand how **event-driven and streaming data pipelines** work and how they differ from traditional batch ETL systems.

---

## 🏗️ Current Architecture

As of Day 3, the project has reached the Kafka producer stage:

```text
E-Commerce Event Generator
          │
          ▼
    Python Kafka Producer
          │
          ▼
   ┌───────────────────┐
   │   Apache Kafka    │
   │                   │
   │ ecommerce-orders │
   └───────────────────┘
          │
          ▼
   Consumer — Next
```

---

## ✅ Current Features

* Synthetic e-commerce event generation
* Realistic customer and product information
* Unique event IDs
* Unique order IDs
* JSON event serialization
* JSONL raw event storage
* Apache Kafka local setup
* KRaft-based Kafka configuration
* Kafka topic creation
* Python Kafka Producer
* Real-time event publishing
* Kafka partition and offset tracking

---

# 📅 Development Progress

| Day    | Implementation                        | Status |
| ------ | ------------------------------------- | ------ |
| Day 1  | Project planning and repository setup | ✅      |
| Day 2  | E-Commerce Event Generator            | ✅      |
| Day 3  | Apache Kafka Setup & Kafka Producer   | ✅      |
| Day 4  | Python Kafka Consumer                 | ⏳      |
| Day 5  | Event Validation & Error Handling     | ⏳      |
| Day 6  | Streaming Data Storage                | ⏳      |
| Day 7  | SQL Analytics Layer                   | ⏳      |
| Day 8  | Real-Time KPI Engine                  | ⏳      |
| Day 9  | Streamlit Dashboard                   | ⏳      |
| Day 10 | Logging & Monitoring                  | ⏳      |
| Day 11 | Automated Testing                     | ⏳      |
| Day 12 | Final Integration & Documentation     | ⏳      |

---

# 📦 E-Commerce Event Generator

The first component of the pipeline is a Python-based event generator.

Instead of relying on a static dataset, the project generates synthetic e-commerce orders that simulate events coming from an online shopping platform.

Each event contains information such as:

```text
event_id
event_type
order_id
customer_id
customer_name
customer_email
city
country
product_id
product_name
category
quantity
unit_price
total_amount
payment_method
order_status
event_timestamp
```

Example event:

```json
{
    "event_type": "ORDER_CREATED",
    "order_id": "ORD-A82D92F612",
    "customer_id": "CUS-991B2C10",
    "product_name": "Laptop",
    "category": "Electronics",
    "quantity": 2,
    "unit_price": 75000,
    "total_amount": 150000,
    "payment_method": "UPI",
    "order_status": "PLACED"
}
```

The values are generated dynamically, so every pipeline run produces different events.

---

# 📄 JSONL Raw Event Storage

Before introducing Kafka, generated events were also stored locally using **JSON Lines (`.jsonl`)**.

```text
data/raw/events.jsonl
```

Each line represents one independent event:

```text
Event 1
Event 2
Event 3
Event 4
...
```

This provides a simple raw record of generated events during early development.

---

# 📨 Apache Kafka Producer

Day 3 introduced **Apache Kafka** into the project.

The Python producer continuously generates e-commerce events and publishes them to Kafka.

```text
generate_event()
       │
       ▼
Python Dictionary
       │
       ▼
JSON Serialization
       │
       ▼
Kafka Producer
       │
       ▼
ecommerce-orders
```

The producer connects to:

```text
localhost:9092
```

and publishes events to:

```text
ecommerce-orders
```

---

# 📨 Kafka Topic

The project currently uses the Kafka topic:

```text
ecommerce-orders
```

This topic acts as the communication channel between the producer and the consumer.

At the current stage:

```text
Producer
   │
   ▼
ecommerce-orders
   │
   ▼
Future Consumer
```

The producer does not need to know how the consumer will process the data. It simply publishes events to the Kafka topic.

This separation is one of the important concepts behind event-driven architectures.

---

# 🔢 Kafka Partitions & Offsets

When an event is successfully published, Kafka provides metadata such as:

```text
Partition: 0
Offset: 0
```

As additional messages are published, offsets increase:

```text
Offset: 0
Offset: 1
Offset: 2
Offset: 3
...
```

Offsets identify the position of messages within a Kafka partition and will become important when the consumer is implemented.

---

# ⚙️ Kafka Setup

The local Kafka environment uses **KRaft mode**, allowing Kafka to manage its metadata without requiring a separate ZooKeeper service.

For the current local development environment, Kafka is configured as a standalone development cluster.

Kafka runs locally on:

```text
localhost:9092
```

The Kafka installation itself is kept outside the GitHub repository and is therefore not part of the project source code.

---

# 🛠️ Current Tech Stack

| Technology   | Purpose                             |
| ------------ | ----------------------------------- |
| Python       | Core pipeline development           |
| Apache Kafka | Event streaming                     |
| kafka-python | Python Kafka integration            |
| Faker        | Synthetic customer/event generation |
| JSON         | Event serialization                 |
| JSONL        | Raw event storage                   |
| Pandas       | Future data processing              |
| PyArrow      | Future Parquet processing           |
| Git          | Version control                     |
| GitHub       | Source code management              |

---

# 📂 Current Project Structure

```text
Real_Time_Ecommerce_Pipeline/
│
├── data/
│   │
│   ├── raw/
│   │   └── events.jsonl
│   │
│   └── processed/
│
├── producer/
│   ├── __init__.py
│   ├── event_generator.py
│   ├── generate_events.py
│   └── kafka_producer.py
│
├── consumer/
│
├── scripts/
│
├── tests/
│
├── README.md
├── requirements.txt
└── .gitignore
```

---

# ▶️ Generate Events Without Kafka

Raw events can still be generated locally using:

```bash
python -m producer.generate_events
```

The generated events are appended to:

```text
data/raw/events.jsonl
```

---

# ▶️ Start Kafka

Kafka must be running before the Kafka producer is executed.

From the local Kafka installation:

```powershell
cd C:\kafka
```

Start the Kafka server:

```powershell
.\bin\windows\kafka-server-start.bat .\config\server.properties
```

Keep this terminal running while working with the streaming pipeline.

---

# ▶️ Check Kafka Topic

In another terminal:

```powershell
cd C:\kafka
```

List available topics:

```powershell
.\bin\windows\kafka-topics.bat --list --bootstrap-server localhost:9092
```

The project topic should appear:

```text
ecommerce-orders
```

---

# ▶️ Run the Kafka Producer

From the project directory:

```bash
python -m producer.kafka_producer
```

The producer generates events and sends them to Kafka.

Example terminal output:

```text
E-Commerce Kafka Producer

Connecting to Kafka: localhost:9092
Topic: ecommerce-orders

[1/20] SENT | ORD-A82D92F612 | Laptop | ₹150,000 | Partition: 0 | Offset: 0

[2/20] SENT | ORD-881AE29321 | Backpack | ₹4,400 | Partition: 0 | Offset: 1

[3/20] SENT | ORD-1F98A23D91 | Smartphone | ₹35,000 | Partition: 0 | Offset: 2
```

---

# 🔎 Verify Kafka Messages

Kafka's console consumer can be used to verify that events are reaching the topic:

```powershell
.\bin\windows\kafka-console-consumer.bat --topic ecommerce-orders --bootstrap-server localhost:9092 --from-beginning
```

The terminal should display the JSON events published by the Python producer.

---

# 💡 What I've Learned So Far

The first three days of this project introduced several important streaming Data Engineering concepts:

* Event-driven architecture
* Synthetic event generation
* JSON event serialization
* Apache Kafka
* Kafka producers
* Kafka brokers
* Kafka topics
* Kafka partitions
* Kafka offsets
* KRaft
* Producer acknowledgements
* Streaming vs batch processing

The biggest change from a traditional ETL project is that the pipeline is beginning to process **events as they occur**, rather than waiting for an entire dataset before processing starts.

---

# 🔜 Next Step — Day 4

The next component will be the **Python Kafka Consumer**.

The architecture will become:

```text
E-Commerce Event Generator
          │
          ▼
     Kafka Producer
          │
          ▼
   ecommerce-orders
      Kafka Topic
          │
          ▼
     Kafka Consumer
          │
          ▼
   Event Processing
```

The consumer will subscribe to the Kafka topic and process incoming order events automatically.

---

# 👨‍💻 Author

**Kartik Dhyani**

Aspiring Data Engineer focused on building practical end-to-end systems using Python, SQL, ETL, Lakehouse, and real-time streaming technologies.

GitHub: [kartikdhyani817](https://github.com/kartikdhyani817)

---

## 🚧 Project Status

**Day 3/12 completed — development in progress.**
