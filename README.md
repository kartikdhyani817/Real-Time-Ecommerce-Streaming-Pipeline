# ⚡ Real-Time E-Commerce Streaming Data Pipeline

A practical **end-to-end Data Engineering project** that simulates an e-commerce platform generating live order events and processes them through a real-time streaming architecture.

The project combines **Python, Apache Kafka, Parquet, DuckDB, SQL, and Streamlit** to demonstrate how data can move from event generation to real-time analytics and visualization.

The project is being built step by step with the goal of understanding the complete lifecycle of streaming data.

---

## 🏗️ Current Architecture

As of **Day 9**, the pipeline is:

```text
E-Commerce Event Generator
          │
          ▼
     Kafka Producer
          │
          ▼
┌─────────────────────────┐
│      Apache Kafka       │
│    ecommerce-orders     │
└────────────┬────────────┘
             │
             ▼
      Kafka Consumer
             │
             ▼
      Event Validator
             │
      ┌──────┴──────┐
      │             │
      ▼             ▼
   INVALID         VALID
      │             │
      ▼             ▼
 Invalid JSONL   Valid JSONL
                    │
                    ▼
              Parquet Writer
                    │
                    ▼
          Partitioned Parquet
             year/month/day
                    │
                    ▼
                 DuckDB
                    │
          ┌─────────┴─────────┐
          │                   │
          ▼                   ▼
     SQL Analytics       KPI Engine
          │                   │
          └─────────┬─────────┘
                    ▼
          Streamlit Dashboard
                    │
                    ▼
           Live Business KPIs
             & Visualizations
