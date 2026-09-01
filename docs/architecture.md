# Cloud Cost Intelligence & Automated Data Engineering Platform

## Phase 1 Architecture: Foundation & Dataset Upload

### Overview
Phase 1 establishes a production-grade foundation for dataset upload, validation, metadata storage, and previewing. Designed for non-coders, it provides an intuitive drag-and-drop React interface backed by a high-performance FastAPI service and PostgreSQL database.

### System Flow
```
User (Browser)
   │
   ▼
React Frontend (Vite, Port 3000)
   │
   │  [HTTP / REST API]
   ▼
Express / FastAPI Backend (Uvicorn, Port 8000)
   │
   ├──► File Storage (`data/uploads/{dataset_id}.csv`)
   │
   └──► PostgreSQL Database (`datasets` table)
```

### Components
- **Frontend**: Single Page Application built with React, Vite, Tailwind CSS v4, Lucide Icons, and Motion.
- **Backend API**: Python FastAPI application managing CSV parsing, SHA-256 idempotency hashing, file sanitization, and dataset metadata.
- **Database**: PostgreSQL (with automatic SQLite fallback for lightweight container preview environments).
- **File Storage**: Local file system storage with safe server-side unique naming (`dataset_<uuid>.csv`).

### Upcoming Phases
- **Phase 2**: Automated Data Cleaning, Preprocessing & Feature Engineering.
- **Phase 3**: Kafka Real-Time Streaming & CDC (Change Data Capture).
- **Phase 4**: Airflow Pipeline Orchestration.
- **Phase 5**: Databricks Delta Lake & Star/Snowflake OLAP Schema.
- **Phase 6**: LLM Cost Insights & Anomaly Detection.
