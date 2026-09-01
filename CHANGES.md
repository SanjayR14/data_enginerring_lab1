# Testing & Verification Changelog

This documents everything found and fixed during an end-to-end testing pass of the
implemented pipeline. Nothing here changes the architecture — same phases, same
technologies (React → FastAPI → Airflow → Kafka → Databricks/Delta → Warehouse →
OLAP → EDA). All items below were verified against live, real data, not just
inspected.

## Startup-blocking bugs (app wouldn't run at all before these)
- `airflow/dags/cloud_cost_pipeline.py`: standalone `DAG` stub class (used when real
  Airflow isn't installed) was missing a `description` parameter the DAG definition
  passes in — crashed on import.
- `backend/requirements.txt`: `kafka-python` is imported in code but was missing
  from the dependency list — crashed the backend on import.
- `backend/app/api/endpoints.py`: `Dict`/`Any` used in type hints but never imported.
- `backend/app/core/config.py`: `python-dotenv` was listed as a dependency but
  `load_dotenv()` was never called — a `.env` file silently did nothing for local
  (non-Docker) runs.

## Data-correctness bugs
- `gold_records` stayed at 0 even after a successful warehouse load — the
  orchestrator only read a `gold_records` key that `fact_load` never returned.
- Feature engineering was missing 3 of 7 required derived fields
  (`forecast_variance`, `cost_per_usage`, `high_budget_utilization_flag`) — added
  using formulas that already existed elsewhere in the codebase but weren't wired
  into the live pipeline.
- `cost_risk_level` / `is_anomaly` / `budget_utilization_pct` were computed with a
  meaningless per-row-net_cost-vs-monthly-budget ratio, and the warehouse stage was
  **overwriting** the correct source values with this wrong ratio. Fixed at both the
  Silver stage and the Gold/fact stage to use the real per-row signals already
  present in the data (`budget_utilization_pct`, `cost_variance_7d_pct`,
  `is_anomaly`, `anomaly_score`).
- `cloud_key` foreign key silently failed to resolve for `Azure` (case mismatch:
  `dim_cloud` stores providers uppercased, the fact-table lookup didn't normalize
  case before matching).
- `clean_data` computed real cleaning/quarantine logic but never persisted its
  output — `feature_engineering` silently re-read raw, uncleaned Bronze data. This
  meant a single bad row (e.g. negative `net_cost`) hard-failed the *entire*
  pipeline instead of being quarantined. Fixed the hand-off between the two stages
  and wired the already-built `QuarantineRecordModel` / `/quarantine` endpoint to
  actually receive data.
- Upload endpoint computed a SHA-256 "idempotency hash" but never checked it —
  uploading the same file twice created two different `dataset_id`s. Fixed:
  identical files now return the existing dataset (`HTTP 200`); genuinely new
  files still create a new one (`HTTP 201`).
- OLAP rollup `interpretation` text showed `'None'` instead of the actual top
  value (wrong dict-key lookup after the groupby column gets remapped, e.g.
  `month` → `month_name`).

## New (explicitly requested) additions
- EDA profile endpoint (`GET /api/datasets/{id}/profile`) now also returns a
  Pearson correlation matrix and IQR-based outlier detection per numeric column,
  plus a duplicate-row count. Frontend (`EdaProfileView.tsx`) updated to display
  these alongside the existing null/distinct/numeric/categorical sections.
- `airflow_service.py` now genuinely prefers a **real** Apache Airflow REST API
  (trigger + poll task instances + pull XCom) when a real Airflow
  scheduler/webserver is reachable, and only falls back to the in-process
  execution engine when it isn't. Previously it attempted the real API call but
  then *always* ran the in-process shim regardless of the result. See
  `docs/REAL_AIRFLOW_SETUP.md` for how to stand up real Airflow against this repo.
- **Known follow-up**: when running through the real-Airflow path, the
  bronze/silver/gold record counts in `/api/pipeline/status` don't populate
  correctly yet (the pipeline itself completes successfully — this is just the
  XCom-based count display for that specific path). Not yet fixed.

## Verified but not modified
- Bronze → Silver → Gold row counts and consistency
- Star schema queries (cost by provider/department/month) and Snowflake-style joins
- OLAP roll-up, drill-down, slice, dice, pivot
- EDA numeric stats, correlation, outliers (cross-checked against independent
  pandas calculations)
- Kafka CDC INSERT → UPDATE → DELETE and duplicate-event dedup (via the app's own
  broker-unavailable fallback path — see note on Kafka below)
- Pipeline idempotency (same dataset reprocessed twice → no duplicate fact rows)

## Known environment limitations (not code bugs)
- **Kafka**: no real broker was reachable in the sandbox this was tested in
  (official Apache Kafka and Redpanda binaries are both hosted on domains outside
  that sandbox's network allowlist, and Docker wasn't available). All CDC logic was
  verified correct through the app's own broker-unavailable fallback path, which
  buffers the event and processes it directly. The `kafka-python` producer/consumer
  code itself is untouched and should work against a real broker unmodified.
- **Databricks**: `databricks_client.py` is structurally correct (verified it
  builds connection config correctly and fails gracefully without credentials) but
  was never tested against a live workspace — needs real
  `DATABRICKS_HOST`/`DATABRICKS_TOKEN`/`DATABRICKS_WAREHOUSE_ID` in `.env`.
