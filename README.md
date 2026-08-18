# ⚡ Real-Time E-Commerce Streaming Data Pipeline

A hands-on **Data Engineering project** that simulates a real-time e-commerce platform and builds an end-to-end streaming pipeline using **Python, Apache Kafka, Parquet, DuckDB, and SQL**.

Instead of working only with static CSV files, this project generates continuous e-commerce order events, streams them through Kafka, validates incoming data, stores clean events in partitioned Parquet files, and calculates analytical KPIs using DuckDB.

The project is being developed step by step to understand how modern **event-driven data pipelines** work in practice.

---

## 🏗️ Current Architecture

As of **Day 8**, the pipeline looks like this:

                         E-Commerce Platform
                                  │
                                  ▼
                         Event Generator
                                  │
                                  ▼
                         Kafka Producer
                                  │
                                  ▼
                        ┌─────────────────┐
                        │  Apache Kafka   │
                        │ ecommerce-orders│
                        └────────┬────────┘
                                 │
                                 ▼
                         Kafka Consumer
                                 │
                                 ▼
                         Event Validator
                                 │
                    ┌────────────┴────────────┐
                    │                         │
                    ▼                         ▼
                 INVALID                    VALID
                    │                         │
                    ▼                         ▼
          invalid_events.jsonl       valid_events.jsonl
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
                                  ┌───────────┴───────────┐
                                  │                       │
                                  ▼                       ▼
                            SQL Analytics          Real-Time KPI
                                                       Engine
                                                         │
                                                         ▼
                                               Auto-Refreshing KPIs

---

# 🎯 Project Goal

The goal of this project is to build a realistic streaming Data Engineering system that demonstrates:

- Event-driven architecture
- Real-time data generation
- Apache Kafka producers and consumers
- Streaming event validation
- Invalid event isolation
- Parquet-based analytical storage
- Date-based data partitioning
- SQL analytics
- DuckDB
- Real-time KPI calculation
- Consumer groups and offsets
- Data quality handling
- Analytical pipeline design

---

# 📅 Development Progress

| Day | Implementation | Status |
|---|---|---|
| Day 1 | Project Setup & Architecture | ✅ |
| Day 2 | E-Commerce Event Generator | ✅ |
| Day 3 | Apache Kafka + Kafka Producer | ✅ |
| Day 4 | Python Kafka Consumer | ✅ |
| Day 5 | Event Validation & Invalid Event Handling | ✅ |
| Day 6 | Partitioned Parquet Storage | ✅ |
| Day 7 | DuckDB + SQL Analytics | ✅ |
| Day 8 | Real-Time KPI Engine | ✅ |
| Day 9 | Real-Time Streamlit Dashboard | ⏳ |
| Day 10 | Logging & Monitoring | ⏳ |
| Day 11 | Automated Testing | ⏳ |
| Day 12 | Final Integration & Documentation | ⏳ |

---

# 📦 Day 2 — E-Commerce Event Generator

The first major component of the project is a Python-based synthetic event generator.

Instead of downloading a static dataset, the project generates realistic e-commerce order events.

Each event contains fields such as:

- Event ID
- Event Type
- Order ID
- Customer ID
- Customer Name
- Customer Email
- City
- Country
- Product ID
- Product Name
- Category
- Quantity
- Unit Price
- Total Amount
- Payment Method
- Order Status
- Event Timestamp

Example:

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
