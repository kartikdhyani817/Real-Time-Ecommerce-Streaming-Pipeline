# Real-Time E-Commerce Streaming Data Pipeline

I built this project to understand how a **real-time Data Engineering pipeline works from end to end**.

Instead of using a static CSV file and running analysis on it, this project simulates an e-commerce system where new orders are continuously generated, sent through **Apache Kafka**, validated, stored as **Parquet**, analyzed using **DuckDB and SQL**, and displayed on a live **Streamlit dashboard**.

I also added logging, health monitoring, invalid-event handling, and automated testing so the project covers more than just moving data from one place to another.

---

## Project Architecture

```text
Synthetic E-Commerce Events
          │
          ▼
     Kafka Producer
          │
          ▼
     Apache Kafka
          │
          ▼
     Kafka Consumer
          │
          ▼
     Event Validation
          │
     ┌────┴────┐
     │         │
     ▼         ▼
  Invalid     Valid
   Events     Events
     │         │
     ▼         ▼
   JSONL    Partitioned
              Parquet
                │
                ▼
             DuckDB
                │
                ▼
           SQL Analytics
                │
                ▼
         Real-Time KPIs
                │
                ▼
      Streamlit Dashboard

   Logging + Monitoring + Testing
        across the pipeline
