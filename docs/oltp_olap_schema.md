# OLTP vs OLAP Architecture & Relational Schema Documentation

## 1. Executive Summary

This document establishes the architecture separation between **OLTP (Online Transaction Processing)** and **OLAP (Online Analytical Processing)** for the Cloud Cost Intelligence & Data Engineering Platform.

- **OLTP Boundary**: Operational, transactional control system running on PostgreSQL (`cloud_cost_db`). Manages dataset uploads, ingestion states, Airflow pipeline execution runs, Kafka event audits, and reference master entities (accounts, projects, budgets). High transaction rates, single-row lookups, normalized structure.
- **OLAP Boundary**: Analytical Data Warehouse running on Databricks Delta Lake (`cloud_cost_catalog.cloud_warehouse` or local Delta storage `./data/delta/warehouse/`). Houses Star Schema and Snowflake Schema models optimized for complex aggregations, multi-dimensional slicing, and financial reporting.

---

## 2. Architecture Diagram

```
                    SOURCE DATA (CSV / Real-Time Streams)
                                     │
                                     ▼
                   ┌───────────────────────────────────┐
                   │    RELATIONAL OLTP (PostgreSQL)   │
                   │ - datasets                        │
                   │ - pipeline_runs                   │
                   │ - airflow_task_instances          │
                   │ - kafka_event_audit               │
                   │ - accounts & projects (Control)   │
                   └───────────────────────────────────┘
                                     │
                        AIRFLOW ORCHESTRATED ETL
                                     │
              ┌──────────────────────┴──────────────────────┐
              │                                             │
              ▼                                             ▼
     BRONZE (Raw Delta)                             SILVER (Clean Delta)
              │                                             │
              └──────────────────────┬──────────────────────┘
                                     │
                                     ▼
                    ┌───────────────────────────────────┐
                    │   DATA WAREHOUSE / OLAP LAYER     │
                    │   cloud_cost_catalog.cloud_warehouse│
                    └───────────────────────────────────┘
                                     │
           ┌─────────────────────────┴─────────────────────────┐
           │                                                   │
           ▼                                                   ▼
 ┌──────────────────────────┐                        ┌──────────────────────────┐
 │       STAR SCHEMA        │                        │     SNOWFLAKE SCHEMA     │
 │ - fact_cloud_cost        │                        │ - fact_cloud_cost        │
 │ - dim_date               │                        │ - dim_organization       │
 │ - dim_cloud              │                        │   ├── dim_business_unit  │
 │ - dim_account            │                        │   ├── dim_department     │
 │ - dim_project            │                        │   └── dim_cost_center    │
 │ - dim_organization       │                        │ - dim_cloud              │
 │ - dim_location           │                        │   └── dim_account        │
 │ - dim_service            │                        │       └── dim_project    │
 │ - dim_environment        │                        └──────────────────────────┘
 │ - dim_currency           │
 └──────────────────────────┘
           │
           ▼
    ANALYTICAL VIEWS (vw_daily_cloud_cost, vw_monthly_cloud_cost, etc.)
           │
           ▼
     FastAPI Endpoints (/api/warehouse/*)  ──►  React Data Warehouse Dashboard
```

---

## 3. Relational OLTP Control Schema (PostgreSQL)

The OLTP schema stores operational state, system logs, batch control, and pipeline audit data.

```
+------------------+         +-----------------------+         +-------------------------------+
|     datasets     | 1     * |     pipeline_runs     | 1     * |    airflow_task_instances     |
+------------------+---------+-----------------------+---------+-------------------------------+
| id (PK)          |         | run_id (PK)           |         | id (PK)                       |
| filename         |         | dataset_id (FK)       |         | dag_id                        |
| original_filename|         | batch_id              |         | dag_run_id                    |
| file_size        |         | dag_run_id            |         | dataset_id                    |
| file_type        |         | status                |         | batch_id                      |
| row_count        |         | current_stage         |         | task_id                       |
| column_count     |         | input_records         |         | status                        |
| status           |         | bronze_records        |         | started_at                    |
| storage_path     |         | valid_records         |         | completed_at                  |
| file_hash        |         | quarantined_records   |         | duration_seconds              |
| columns_json     |         | silver_records        |         | error_message                 |
| upload_timestamp |         | gold_records          |         | xcom_data                     |
+------------------+         | failed_stage          |         +-------------------------------+
                             | error_message         |
                             | started_at            |
                             | completed_at          |
                             +-----------------------+
                                         │
                                         │ 1
                                         │
                                         │ *
                             +-----------------------+
                             |   kafka_event_audit   |
                             +-----------------------+
                             | event_id (PK)         |
                             | dataset_id (FK)       |
                             | event_type            |
                             | status                |
                             | payload_json          |
                             | received_at           |
                             +-----------------------+
```

---

## 4. OLTP vs OLAP Comparison Matrix

| Property | OLTP (PostgreSQL Operational Control) | OLAP (Databricks Delta Data Warehouse) |
|---|---|---|
| **Primary Goal** | High-concurrency transactional processing & pipeline orchestration | Multi-dimensional analytics, business intelligence, financial aggregation |
| **Data Structure** | Highly normalized (3NF) relational tables | Dimensional modeling (Star & Snowflake schemas) |
| **Read/Write Pattern** | Small, frequent read/write transactions per request | Massively parallel batch/stream inserts & multi-dimensional queries |
| **Key Entities** | `datasets`, `pipeline_runs`, `airflow_task_instances`, `kafka_event_audit` | `fact_cloud_cost`, `dim_date`, `dim_cloud`, `dim_account`, `dim_project`, `dim_organization`, `dim_location`, `dim_service`, `dim_environment`, `dim_currency` |
| **Key Types** | Natural string UUID keys (`dataset_id`, `run_id`) | Integer Surrogate Keys (`date_key`, `account_key`, `project_key`, etc.) |
| **Query Types** | `SELECT * FROM pipeline_runs WHERE dataset_id = ?` | `SELECT year, cloud_provider, SUM(net_cost) FROM fact_cloud_cost JOIN ... GROUP BY ...` |
