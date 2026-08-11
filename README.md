# ⚡ Real-Time E-Commerce Streaming Data Pipeline

An end-to-end **Data Engineering project** focused on building a real-time event processing pipeline for an e-commerce platform.

The project will simulate live customer and order events, stream them through **Apache Kafka**, process and validate incoming events using Python, store processed data for analytics, and provide near-real-time business insights through an interactive dashboard.

The main goal of this project is to learn how **streaming Data Engineering systems** work and how they differ from traditional batch ETL pipelines.

---

## 🎯 Project Goal

Traditional pipelines usually process data in batches.

This project focuses on continuously generated events such as:

- Customer orders
- Product purchases
- Payment events
- Order status changes
- Customer activity

Instead of waiting for a complete dataset, events will be processed as they arrive.

---

## 🏗️ Planned Architecture

```text
E-Commerce Event Generator
          │
          ▼
     Apache Kafka
          │
          ▼
    Python Consumer
          │
          ▼
    Data Validation
          │
     ┌────┴────┐
     ▼         ▼
  Parquet     SQL
  Storage   Analytics
     │         │
     └────┬────┘
          ▼
    Real-Time KPIs
          │
          ▼
 Streamlit Dashboard
```

---

## 🛠️ Planned Tech Stack

- Python
- Apache Kafka
- SQL
- Pandas
- Parquet
- PyArrow
- Faker
- Streamlit
- Plotly
- Pytest
- Git & GitHub

The project is being designed around **free and locally available technologies**.

---

## 📂 Initial Project Structure

```text
Real_Time_Ecommerce_Pipeline/
│
├── data/
│   ├── raw/
│   └── processed/
│
├── producer/
├── consumer/
├── scripts/
├── tests/
│
├── README.md
├── requirements.txt
└── .gitignore
```

---

## 🚀 Planned Features

- Real-time e-commerce event generation
- Apache Kafka producer
- Kafka consumer
- JSON event processing
- Data validation
- Invalid event handling
- Parquet storage
- SQL analytics
- Real-time KPI calculation
- Streamlit monitoring dashboard
- Pipeline logging
- Automated testing

---

## 📅 Development Progress

| Day | Implementation | Status |
|---|---|---|
| Day 1 | Project planning and repository setup | ✅ |
| Day 2 | E-Commerce Event Generator | ⏳ |
| Day 3 | Kafka Producer | ⏳ |
| Day 4 | Kafka Consumer | ⏳ |
| Day 5 | Event Validation & Error Handling | ⏳ |
| Day 6 | Streaming Data Storage | ⏳ |
| Day 7 | SQL Analytics Layer | ⏳ |
| Day 8 | Real-Time KPI Engine | ⏳ |
| Day 9 | Streamlit Dashboard | ⏳ |
| Day 10 | Logging & Monitoring | ⏳ |
| Day 11 | Automated Testing | ⏳ |
| Day 12 | Final Integration & Documentation | ⏳ |

---

## 👨‍💻 Author

**Kartik Dhyani**

Aspiring Data Engineer focused on building practical end-to-end data pipelines using Python, SQL, ETL, Lakehouse, and streaming technologies.

GitHub: [kartikdhyani817](https://github.com/kartikdhyani817)

---

## 🚧 Project Status

**Day 1/12 — Development in progress.**
