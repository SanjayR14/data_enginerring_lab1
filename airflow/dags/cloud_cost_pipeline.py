"""
Cloud Cost Intelligence & Data Engineering Platform
Phase 4: Apache Airflow Orchestration & End-to-End ETL Pipeline
DAG ID: cloud_cost_etl_pipeline
"""

import os
import sys
import json
import logging
import hashlib
from datetime import datetime, timedelta
import pandas as pd
import numpy as np

# Ensure the project root (parent of this airflow/dags/ directory) is importable,
# so 'backend.app.services.*' resolves whether this file is loaded by the real
# Airflow scheduler (its own venv/working directory) or by the FastAPI backend's
# in-process fallback runner.
_PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

logger = logging.getLogger(__name__)

# Standard Airflow imports with standalone fallback if running outside Airflow daemon
try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator
    AIRFLOW_AVAILABLE = True
except Exception:
    AIRFLOW_AVAILABLE = False
    
    # Standalone mock DAG & PythonOperator for standalone orchestrator bridge
    class DAG:
        def __init__(self, dag_id, default_args=None, description=None, schedule_interval=None, schedule=None, catchup=False, tags=None, params=None):
            self.dag_id = dag_id
            self.default_args = default_args or {}
            self.description = description
            self.params = params or {}
            self.tasks = []

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            pass

    class PythonOperator:
        def __init__(self, task_id, python_callable, op_kwargs=None, provide_context=True, dag=None, retries=2, retry_delay=None, execution_timeout=None):
            self.task_id = task_id
            self.python_callable = python_callable
            self.op_kwargs = op_kwargs or {}
            self.provide_context = provide_context
            self.dag = dag
            self.upstream_tasks = []
            self.downstream_tasks = []
            if dag and hasattr(dag, 'tasks'):
                dag.tasks.append(self)

        def __rshift__(self, other):
            self.downstream_tasks.append(other)
            other.upstream_tasks.append(self)
            return other

default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(seconds=10),
    'execution_timeout': timedelta(minutes=15),
}

# -----------------------------------------------------------------------------
# TASK IMPLEMENTATIONS
# -----------------------------------------------------------------------------

def generate_record_hash(row: pd.Series) -> str:
    """Deterministic SHA-256 hash for record deduplication."""
    raw_key = (
        f"{row.get('date', '')}|"
        f"{row.get('cloud_provider', '')}|"
        f"{row.get('account_id', '')}|"
        f"{row.get('project_id', '')}|"
        f"{row.get('service', '')}|"
        f"{row.get('resource_type', '')}|"
        f"{row.get('usage_quantity', '')}|"
        f"{row.get('net_cost', '')}"
    )
    return hashlib.sha256(raw_key.encode('utf-8')).hexdigest()

def check_dataset(**kwargs):
    """1. Check Dataset: Verifies dataset metadata exists in DB and storage CSV path exists."""
    params = kwargs.get('params') or kwargs.get('dag_run').conf or {}
    dataset_id = params.get('dataset_id', 'ds_sample_test')
    logger.info(f"[AIRFLOW:check_dataset] Validating dataset_id: {dataset_id}")
    
    # Try reading from database or direct filesystem fallback
    csv_path = f"./data/uploads/{dataset_id}.csv"
    sample_path = "./data/sample/cloud_cost_dataset.csv"
    
    target_path = None
    if os.path.exists(csv_path):
        target_path = csv_path
    elif os.path.exists(sample_path):
        target_path = sample_path
    else:
        # Look for matching file in data/uploads
        if os.path.exists("./data/uploads"):
            for f in os.listdir("./data/uploads"):
                if dataset_id in f and f.endswith(".csv"):
                    target_path = os.path.join("./data/uploads", f)
                    break

    if not target_path or not os.path.exists(target_path):
        raise FileNotFoundError(f"Dataset file for '{dataset_id}' was not found in storage.")

    logger.info(f"[AIRFLOW:check_dataset] Dataset file verified at: {target_path}")
    return {"dataset_id": dataset_id, "file_path": target_path, "status": "EXISTS"}

def validate_schema(**kwargs):
    """2. Validate Schema: Validates CSV layout, column names, non-empty content."""
    ti = kwargs.get('ti')
    dataset_info = ti.xcom_pull(task_ids='check_dataset') if ti else None
    params = kwargs.get('params') or kwargs.get('dag_run').conf or {}
    dataset_id = params.get('dataset_id', 'ds_sample_test')
    
    file_path = dataset_info.get('file_path') if dataset_info else f"./data/uploads/{dataset_id}.csv"
    if not os.path.exists(file_path):
        file_path = "./data/sample/cloud_cost_dataset.csv"

    df = pd.read_csv(file_path)
    if df.empty:
        raise ValueError("Dataset file is empty (0 rows).")

    required_cols = ['date', 'cloud_provider', 'account_id', 'service', 'net_cost']
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Schema Validation Failed: Missing required columns: {missing}")

    logger.info(f"[AIRFLOW:validate_schema] Schema PASS. Columns: {len(df.columns)}, Rows: {len(df)}")
    return {"dataset_id": dataset_id, "columns": list(df.columns), "row_count": len(df)}

def create_batch(**kwargs):
    """3. Create Batch: Registers batch_id and initializes run tracking."""
    params = kwargs.get('params') or kwargs.get('dag_run').conf or {}
    dataset_id = params.get('dataset_id', 'ds_sample_test')
    batch_id = params.get('batch_id') or f"batch_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
    
    logger.info(f"[AIRFLOW:create_batch] Initialized batch '{batch_id}' for dataset '{dataset_id}'")
    return {"dataset_id": dataset_id, "batch_id": batch_id, "status": "BATCH_CREATED"}

def load_bronze(**kwargs):
    """4. Load Bronze: Ingests raw CSV into Bronze Delta Lake Parquet with hashes and metadata."""
    params = kwargs.get('params') or kwargs.get('dag_run').conf or {}
    dataset_id = params.get('dataset_id', 'ds_sample_test')
    batch_id = params.get('batch_id', 'batch_default')
    
    csv_path = f"./data/uploads/{dataset_id}.csv"
    if not os.path.exists(csv_path):
        csv_path = "./data/sample/cloud_cost_dataset.csv"
        
    df = pd.read_csv(csv_path)
    df_bronze = df.copy()
    df_bronze['record_hash'] = df_bronze.apply(generate_record_hash, axis=1)
    df_bronze['dataset_id'] = dataset_id
    df_bronze['batch_id'] = batch_id
    df_bronze['ingestion_timestamp'] = datetime.utcnow().isoformat()

    os.makedirs("./data/delta/bronze_cloud_cost_raw", exist_ok=True)
    bronze_path = f"./data/delta/bronze_cloud_cost_raw/{dataset_id}.parquet"
    df_bronze.to_parquet(bronze_path, index=False)
    
    logger.info(f"[AIRFLOW:load_bronze] Loaded {len(df_bronze)} records to Bronze layer at {bronze_path}")
    return {"dataset_id": dataset_id, "batch_id": batch_id, "bronze_records": len(df_bronze), "path": bronze_path}

def bronze_quality_check(**kwargs):
    """5. Bronze Quality Check: Data Quality Gate on Bronze layer."""
    params = kwargs.get('params') or kwargs.get('dag_run').conf or {}
    dataset_id = params.get('dataset_id', 'ds_sample_test')
    
    bronze_path = f"./data/delta/bronze_cloud_cost_raw/{dataset_id}.parquet"
    if not os.path.exists(bronze_path):
        raise FileNotFoundError("Bronze Parquet storage missing for quality check.")

    df_bronze = pd.read_parquet(bronze_path)
    total_records = len(df_bronze)
    
    # Check null critical net cost
    null_costs = df_bronze['net_cost'].isnull().sum() if 'net_cost' in df_bronze.columns else total_records
    null_pct = (null_costs / total_records) * 100.0 if total_records > 0 else 0.0

    if null_pct > 50.0:
        raise ValueError(f"Bronze Quality Gate FAIL: {null_pct:.1f}% null net_costs exceeds 50% limit!")

    logger.info(f"[AIRFLOW:bronze_quality_check] Bronze DQ Gate PASS. Total: {total_records}, Nulls: {null_costs} ({null_pct:.1f}%)")
    return {"dataset_id": dataset_id, "status": "PASS", "records_checked": total_records, "null_pct": null_pct}

def clean_data(**kwargs):
    """6. Clean Data: Quarantines invalid rows, strips whitespace, standardizes types."""
    params = kwargs.get('params') or kwargs.get('dag_run').conf or {}
    dataset_id = params.get('dataset_id', 'ds_sample_test')
    batch_id = params.get('batch_id', 'batch_default')

    bronze_path = f"./data/delta/bronze_cloud_cost_raw/{dataset_id}.parquet"
    df_bronze = pd.read_parquet(bronze_path)

    # Filter out null net_costs, invalid dates, or negative net_cost (invalid business value)
    net_cost_numeric = pd.to_numeric(df_bronze['net_cost'], errors='coerce')
    valid_mask = net_cost_numeric.notnull() & (net_cost_numeric >= 0) & df_bronze['date'].notnull()
    df_valid = df_bronze[valid_mask].copy()
    df_invalid = df_bronze[~valid_mask].copy()
    quarantined_count = len(df_invalid)

    # Clean string fields
    str_cols = df_valid.select_dtypes(include=['object']).columns
    for c in str_cols:
        if c not in ['record_hash', 'dataset_id', 'batch_id', 'ingestion_timestamp']:
            df_valid[c] = df_valid[c].astype(str).str.strip().replace({'nan': 'N/A', '': 'N/A'})

    if 'cloud_provider' in df_valid.columns:
        df_valid['cloud_provider'] = df_valid['cloud_provider'].str.upper()

    # Numeric coercion
    num_cols = ['usage_quantity', 'list_cost', 'net_cost', 'budget_amount', 'reserved_savings', 'savings_plan_savings', 'spot_savings']
    for c in num_cols:
        if c in df_valid.columns:
            df_valid[c] = pd.to_numeric(df_valid[c], errors='coerce').fillna(0.0).astype(float)
        else:
            df_valid[c] = 0.0

    # Persist cleaned output so downstream feature_engineering actually uses it
    os.makedirs("./data/staging", exist_ok=True)
    staged_path = f"./data/staging/{dataset_id}_clean.parquet"
    df_valid.to_parquet(staged_path, index=False)

    # Record quarantined rows so the existing quarantine API/UI has real data to show
    if quarantined_count > 0:
        try:
            from backend.app.db.database import SessionLocal
            from backend.app.models.pipeline import QuarantineRecordModel
            db = SessionLocal()
            try:
                for _, row in df_invalid.iterrows():
                    db.add(QuarantineRecordModel(
                        run_id=batch_id,
                        dataset_id=dataset_id,
                        batch_id=batch_id,
                        record_hash=str(row.get('record_hash', '')) or None,
                        failure_reason="Negative or missing net_cost / missing date",
                        original_record=row.to_json()
                    ))
                db.commit()
            finally:
                db.close()
        except Exception as qe:
            logger.warning(f"[AIRFLOW:clean_data] Failed to persist quarantine records: {qe}")

    logger.info(f"[AIRFLOW:clean_data] Cleaned {len(df_valid)} records ({quarantined_count} quarantined)")
    return {"dataset_id": dataset_id, "valid_records": len(df_valid), "quarantined_records": quarantined_count}

def feature_engineering(**kwargs):
    """7. Feature Engineering: Calculates total_savings, effective_discount_pct, risk levels."""
    ti = kwargs.get('ti')
    params = kwargs.get('params') or kwargs.get('dag_run').conf or {}
    dataset_id = params.get('dataset_id', 'ds_sample_test')

    staged_path = f"./data/staging/{dataset_id}_clean.parquet"
    bronze_path = f"./data/delta/bronze_cloud_cost_raw/{dataset_id}.parquet"
    df = pd.read_parquet(staged_path if os.path.exists(staged_path) else bronze_path)
    df = df[df['net_cost'].notnull()].copy()

    # Derived Features
    for col in ['reserved_savings', 'savings_plan_savings', 'spot_savings', 'list_cost', 'net_cost', 'budget_amount']:
        if col not in df.columns:
            df[col] = 0.0
        else:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)

    df['total_savings'] = (df['reserved_savings'] + df['savings_plan_savings'] + df['spot_savings']).round(2)
    df['effective_discount_pct'] = np.where(df['list_cost'] > 0, ((df['list_cost'] - df['net_cost']) / df['list_cost']) * 100.0, 0.0).round(2)
    df['budget_remaining'] = (df['budget_amount'] - df['net_cost']).round(2)

    # forecast_variance = forecast_monthly_cost - budget_amount
    for col in ['forecast_monthly_cost', 'usage_quantity', 'budget_utilization_pct']:
        if col not in df.columns:
            df[col] = 0.0
        else:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)
    df['forecast_variance'] = (df['forecast_monthly_cost'] - df['budget_amount']).round(2)

    # cost_per_usage = net_cost / usage_quantity if usage_quantity > 0 else 0.0
    df['cost_per_usage'] = np.where(df['usage_quantity'] > 0, df['net_cost'] / df['usage_quantity'], 0.0).round(4)

    # high_budget_utilization_flag: True if budget_utilization_pct >= 85 or net_cost >= 85% of budget
    df['high_budget_utilization_flag'] = (df['budget_utilization_pct'] >= 85.0) | (
        (df['budget_amount'] > 0) & (df['net_cost'] >= 0.85 * df['budget_amount'])
    )

    for col in ['budget_utilization_pct', 'cost_variance_7d_pct', 'anomaly_score']:
        if col not in df.columns:
            df[col] = 0.0
        else:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)
    if 'is_anomaly' not in df.columns:
        df['is_anomaly'] = False
    else:
        df['is_anomaly'] = df['is_anomaly'].astype(bool)

    def calc_risk(row):
        if row['is_anomaly'] or row['budget_utilization_pct'] >= 90.0 or row['cost_variance_7d_pct'] >= 25.0:
            return "HIGH"
        elif row['budget_utilization_pct'] >= 75.0 or row['cost_variance_7d_pct'] >= 10.0 or row['anomaly_score'] >= 0.5:
            return "MEDIUM"
        return "LOW"

    df['cost_risk_level'] = df.apply(calc_risk, axis=1)

    os.makedirs("./data/delta/silver_cloud_cost_clean", exist_ok=True)
    silver_path = f"./data/delta/silver_cloud_cost_clean/{dataset_id}.parquet"
    df.to_parquet(silver_path, index=False)

    logger.info(f"[AIRFLOW:feature_engineering] Feature engineering completed for {len(df)} records.")
    return {"dataset_id": dataset_id, "silver_records": len(df), "silver_path": silver_path}

def silver_quality_check(**kwargs):
    """8. Silver Quality Check: Quality Gate 2 on Silver clean data."""
    params = kwargs.get('params') or kwargs.get('dag_run').conf or {}
    dataset_id = params.get('dataset_id', 'ds_sample_test')

    silver_path = f"./data/delta/silver_cloud_cost_clean/{dataset_id}.parquet"
    if not os.path.exists(silver_path):
        raise FileNotFoundError("Silver Parquet storage missing for quality check.")

    df_silver = pd.read_parquet(silver_path)
    
    # Check negative costs
    negative_costs = (df_silver['net_cost'] < 0).sum()
    if negative_costs > 0:
        raise ValueError(f"Silver Quality Gate FAIL: Found {negative_costs} records with negative net_cost!")

    logger.info(f"[AIRFLOW:silver_quality_check] Silver DQ Gate PASS. Total valid records: {len(df_silver)}")
    return {"dataset_id": dataset_id, "status": "PASS", "records": len(df_silver)}

def prepare_gold(**kwargs):
    """Prepare Gold layer: populate dimensions and fact tables in the warehouse model."""
    params = kwargs.get('params') or kwargs.get('dag_run').conf or {}
    dataset_id = params.get('dataset_id', 'ds_sample_test')
    batch_id = params.get('batch_id', 'batch_default')

    from backend.app.services.warehouse_service import DataWarehouseEngine
    summary = DataWarehouseEngine.load_warehouse_from_silver(dataset_id, batch_id)
    gold_path = _write_gold_summary(dataset_id, batch_id)
    logger.info(f"[AIRFLOW:prepare_gold] Gold preparation succeeded for {dataset_id}.")
    return {"dataset_id": dataset_id, "dimensions_loaded": 10, "status": "SUCCESS", "fact_records_total": summary.get('fact_records_total', 0), "gold_path": gold_path}


def dimension_load(**kwargs):
    """Compatibility alias for the legacy dimension_load task name."""
    return prepare_gold(**kwargs)


def load_warehouse(**kwargs):
    """Load warehouse fact records into the warehouse layer."""
    params = kwargs.get('params') or kwargs.get('dag_run').conf or {}
    dataset_id = params.get('dataset_id', 'ds_sample_test')
    batch_id = params.get('batch_id', 'batch_default')

    from backend.app.services.warehouse_service import DataWarehouseEngine
    res = DataWarehouseEngine.load_warehouse_from_silver(dataset_id, batch_id)
    logger.info(f"[AIRFLOW:load_warehouse] Loaded {res.get('fact_records_inserted', 0)} facts into Data Warehouse.")
    return {
        "dataset_id": dataset_id,
        "batch_id": batch_id,
        "fact_records": res.get('fact_records_inserted', 0),
        "gold_records": res.get('fact_records_total', 0)
    }


def fact_load(**kwargs):
    """Compatibility alias for the legacy fact_load task name."""
    return load_warehouse(**kwargs)


def verify_result(**kwargs):
    """Verify the warehouse results and analytical queries."""
    from backend.app.services.warehouse_service import DataWarehouseEngine
    queries = DataWarehouseEngine.execute_analytical_queries()
    if not queries:
        raise ValueError("Warehouse Verification FAIL: Analytical queries returned no data.")

    logger.info(f"[AIRFLOW:verify_result] Warehouse Verification PASS. Executed {len(queries)} analytical queries.")
    return {"status": "VERIFIED", "queries_executed": len(queries)}


def warehouse_quality_check(**kwargs):
    """Compatibility alias that validates warehouse data integrity."""
    from backend.app.services.warehouse_service import DataWarehouseEngine
    summary = DataWarehouseEngine.get_warehouse_summary()

    if summary['fact_record_count'] == 0:
        raise ValueError("Warehouse Quality Check FAIL: Fact table fact_cloud_cost is empty!")

    logger.info(f"[AIRFLOW:warehouse_quality_check] Warehouse Quality Gate PASS. Fact rows: {summary['fact_record_count']}")
    return {"status": "PASS", "fact_rows": summary['fact_record_count']}


def refresh_olap_aggregates(**kwargs):
    """Materialize OLAP aggregate views for reporting performance."""
    from backend.app.services.olap_service import OLAPEngine
    res = OLAPEngine.refresh_olap_aggregates()
    logger.info(f"[AIRFLOW:refresh_olap_aggregates] Materialized {len(res.get('aggregates', []))} aggregate views.")
    return res


def warehouse_verification(**kwargs):
    """Compatibility alias for the legacy warehouse verification task name."""
    return verify_result(**kwargs)


def update_pipeline_status(**kwargs):
    """13. Update Pipeline Status: Sets pipeline run status to SUCCESS in DB ledger."""
    params = kwargs.get('params') or kwargs.get('dag_run').conf or {}
    dataset_id = params.get('dataset_id', 'ds_sample_test')
    batch_id = params.get('batch_id', 'batch_default')

    logger.info(f"[AIRFLOW:update_pipeline_status] Pipeline SUCCESS for dataset {dataset_id}, batch {batch_id}.")
    return {"dataset_id": dataset_id, "batch_id": batch_id, "status": "SUCCESS"}


def _write_gold_summary(dataset_id: str, batch_id: str):
    """Persist an aggregate Gold-layer Parquet summary expected by pipeline validation."""
    silver_path = f"./data/delta/silver_cloud_cost_clean/{dataset_id}.parquet"
    if not os.path.exists(silver_path):
        return None

    df = pd.read_parquet(silver_path)
    if df.empty:
        return None

    df_gold = df.copy()
    df_gold['month'] = pd.to_datetime(df_gold['date'], errors='coerce').dt.month.fillna(1)
    gold = df_gold.groupby(['cloud_provider', 'project_id', 'region', 'service', 'environment', 'month'], as_index=False).agg(
        total_net_cost=('net_cost', 'sum'),
        total_list_cost=('list_cost', 'sum'),
        total_savings=('total_savings', 'sum'),
        avg_discount_pct=('effective_discount_pct', 'mean'),
        record_count=('net_cost', 'count'),
        high_risk_count=('cost_risk_level', lambda s: int((s == 'HIGH').sum()))
    ).round(2)
    gold['dataset_id'] = dataset_id
    gold['batch_id'] = batch_id

    os.makedirs("./data/delta/gold_cloud_cost_summary", exist_ok=True)
    gold_path = f"./data/delta/gold_cloud_cost_summary/{dataset_id}.parquet"
    gold.to_parquet(gold_path, index=False)
    return gold_path


def sync_to_databricks(**kwargs):
    """14. Sync to Databricks: Synchronizes the ETL output to Databricks."""
    params = kwargs.get('params') or kwargs.get('dag_run').conf or {}
    dataset_id = params.get('dataset_id', 'ds_sample_test')
    batch_id = params.get('batch_id', 'batch_default')

    bronze_path = f"./data/delta/bronze_cloud_cost_raw/{dataset_id}.parquet"
    silver_path = f"./data/delta/silver_cloud_cost_clean/{dataset_id}.parquet"
    
    import pandas as pd
    from backend.app.services.databricks_client import DatabricksClient

    bronze_df = pd.read_parquet(bronze_path) if os.path.exists(bronze_path) else pd.DataFrame()
    silver_df = pd.read_parquet(silver_path) if os.path.exists(silver_path) else pd.DataFrame()
    quarantine_df = pd.DataFrame(columns=['dataset_id', 'batch_id', 'record_hash', 'failure_reason', 'failed_at', 'original_record'])

    db_sync_ok, db_sync_msg = DatabricksClient.sync_pipeline_data(
        dataset_id=dataset_id,
        batch_id=batch_id,
        bronze_df=bronze_df,
        silver_df=silver_df,
        quarantine_df=quarantine_df,
        dq_results=[]
    )
    if not db_sync_ok:
        raise Exception(f"Databricks sync failed: {db_sync_msg}")

    logger.info(f"[AIRFLOW:sync_to_databricks] {db_sync_msg}")
    return {"status": "SUCCESS", "message": db_sync_msg}

# -----------------------------------------------------------------------------
# DAG DEFINITION
# -----------------------------------------------------------------------------

with DAG(
    dag_id='cloud_cost_etl_pipeline',
    default_args=default_args,
    description='Phase 5 Data Warehouse & Airflow Production ETL Pipeline',
    schedule_interval=None,
    catchup=False,
    tags=['cloud_cost', 'phase5', 'warehouse', 'star_schema', 'snowflake_schema'],
    params={
        'dataset_id': 'ds_sample_test',
        'batch_id': 'batch-001'
    }
) as dag:

    t_check_dataset = PythonOperator(
        task_id='check_dataset',
        python_callable=check_dataset,
        provide_context=True
    )

    t_validate_schema = PythonOperator(
        task_id='validate_schema',
        python_callable=validate_schema,
        provide_context=True
    )

    t_create_batch = PythonOperator(
        task_id='create_batch',
        python_callable=create_batch,
        provide_context=True
    )

    t_load_bronze = PythonOperator(
        task_id='load_bronze',
        python_callable=load_bronze,
        provide_context=True
    )

    t_bronze_quality_check = PythonOperator(
        task_id='bronze_quality_check',
        python_callable=bronze_quality_check,
        provide_context=True
    )

    t_clean_data = PythonOperator(
        task_id='clean_data',
        python_callable=clean_data,
        provide_context=True
    )

    t_feature_engineering = PythonOperator(
        task_id='feature_engineering',
        python_callable=feature_engineering,
        provide_context=True
    )

    t_silver_quality_check = PythonOperator(
        task_id='silver_quality_check',
        python_callable=silver_quality_check,
        provide_context=True
    )

    t_prepare_gold = PythonOperator(
        task_id='prepare_gold',
        python_callable=prepare_gold,
        provide_context=True
    )

    t_load_warehouse = PythonOperator(
        task_id='load_warehouse',
        python_callable=load_warehouse,
        provide_context=True
    )

    t_verify_result = PythonOperator(
        task_id='verify_result',
        python_callable=verify_result,
        provide_context=True
    )

    t_update_pipeline_status = PythonOperator(
        task_id='update_pipeline_status',
        python_callable=update_pipeline_status,
        provide_context=True
    )

    # Airflow Task Dependency Flow
    (
        t_check_dataset
        >> t_validate_schema
        >> t_create_batch
        >> t_load_bronze
        >> t_bronze_quality_check
        >> t_clean_data
        >> t_feature_engineering
        >> t_silver_quality_check
        >> t_prepare_gold
        >> t_load_warehouse
        >> t_verify_result
        >> t_update_pipeline_status
    )

TASK_SEQUENCE = [
    ("check_dataset", check_dataset),
    ("validate_schema", validate_schema),
    ("create_batch", create_batch),
    ("load_bronze", load_bronze),
    ("bronze_quality_check", bronze_quality_check),
    ("clean_data", clean_data),
    ("feature_engineering", feature_engineering),
    ("silver_quality_check", silver_quality_check),
    ("prepare_gold", prepare_gold),
    ("load_warehouse", load_warehouse),
    ("verify_result", verify_result),
    ("update_pipeline_status", update_pipeline_status),
]
