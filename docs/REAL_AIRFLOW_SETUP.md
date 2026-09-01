# Running This Pipeline on Real Apache Airflow

The app works without this (it falls back to an in-process execution engine that
runs the same task functions sequentially), but here's how to get it running on an
actual Airflow scheduler + webserver, which is what `backend/app/services/airflow_service.py`
prefers when it's reachable.

This was verified working end-to-end: real scheduler, real webserver, real DAG
discovery, a real triggered run with all 14 tasks completing successfully with
real per-task durations, real XCom, correct data in Bronze/Silver/Gold.

## 1. Install Airflow in its own virtualenv

Airflow pins its own dependency versions (notably `SQLAlchemy<2.0`), which will
conflict with this project's own dependencies (`SQLAlchemy>=2.0.28`) if installed
into the same environment. Keep them separate:

```bash
python3 -m venv ~/airflow_venv
export AIRFLOW_HOME=~/airflow_home
AIRFLOW_VERSION=2.10.5
PYTHON_VERSION=3.12   # match your python3 --version
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
~/airflow_venv/bin/pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "$CONSTRAINT_URL"
~/airflow_venv/bin/pip install psycopg2-binary
```

The DAG's task functions (`airflow/dags/cloud_cost_pipeline.py`) also need this
project's data libraries available to Airflow's own Python interpreter — install
these into the **same** Airflow venv, pinning SQLAlchemy back down afterward since
`apache-airflow` needs `<2.0`:

```bash
~/airflow_venv/bin/pip install "sqlalchemy<2.0"
~/airflow_venv/bin/pip install pandas numpy pyarrow duckdb databricks-sql-connector requests kafka-python
```

## 2. Point Airflow's metadata DB at Postgres and this repo's DAGs folder

```bash
export AIRFLOW_HOME=~/airflow_home
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="postgresql+psycopg2://postgres:postgres@127.0.0.1:5432/airflow_db"
export AIRFLOW__CORE__DAGS_FOLDER="/path/to/this/repo/airflow/dags"
export AIRFLOW__CORE__LOAD_EXAMPLES="False"
export AIRFLOW__CORE__EXECUTOR="LocalExecutor"

psql -h 127.0.0.1 -U postgres -c "CREATE DATABASE airflow_db;"
~/airflow_venv/bin/airflow db migrate
~/airflow_venv/bin/airflow users create \
  --username airflow --firstname Airflow --lastname Service \
  --role Admin --email airflow@example.com --password airflow
```

The username/password above match this repo's `.env.example` defaults
(`AIRFLOW_USERNAME`/`AIRFLOW_PASSWORD`) — change both together if you use
different credentials.

## 3. Start the scheduler and webserver from the repo root

Task subprocesses inherit the scheduler's working directory, and the DAG's task
functions use relative paths (`./data/delta/...`) — so start Airflow **from this
repo's root directory**, the same way the FastAPI backend does:

```bash
cd /path/to/this/repo
export AIRFLOW_HOME=~/airflow_home
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="postgresql+psycopg2://postgres:postgres@127.0.0.1:5432/airflow_db"
export AIRFLOW__CORE__DAGS_FOLDER="$(pwd)/airflow/dags"
export AIRFLOW__CORE__EXECUTOR="LocalExecutor"
export AIRFLOW__API__AUTH_BACKENDS="airflow.api.auth.backend.basic_auth"

~/airflow_venv/bin/airflow scheduler &
~/airflow_venv/bin/airflow webserver --port 8080 &
```

Give it ~15-20 seconds, then confirm:

```bash
curl -u airflow:airflow http://localhost:8080/api/v1/dags/cloud_cost_etl_pipeline
```

## 4. Unpause the DAG

New DAGs start paused:

```bash
curl -u airflow:airflow -X PATCH http://localhost:8080/api/v1/dags/cloud_cost_etl_pipeline \
  -H "Content-Type: application/json" -d '{"is_paused": false}'
```

## 5. That's it

With `AIRFLOW_URL=http://localhost:8080` in `.env` (already the default) and the
webserver reachable, `POST /api/pipeline/process/{dataset_id}` will now trigger a
real DAG run and the app will poll real task states/XCom instead of running the
in-process fallback. Check the FastAPI backend log for
`[AIRFLOW REST] Successfully triggered real Airflow DAG run ... via REST API` to
confirm it took the real path.

**Known issue**: bronze/silver/gold record counts in `/api/pipeline/status` don't
currently populate correctly when running via this real-Airflow path (the pipeline
itself completes correctly — this is just the count display). See `CHANGES.md`.
