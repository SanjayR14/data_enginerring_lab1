from sqlalchemy import Column, String, Integer, DateTime, Text, Boolean, Float
from datetime import datetime
import uuid
from backend.app.db.database import Base

class PipelineRunModel(Base):
    __tablename__ = "pipeline_runs"

    run_id = Column(String, primary_key=True, index=True, default=lambda: f"run_{uuid.uuid4().hex[:12]}")
    dataset_id = Column(String, index=True, nullable=False)
    batch_id = Column(String, index=True, nullable=False)
    dag_run_id = Column(String, index=True, nullable=True)
    status = Column(String, nullable=False, default="QUEUED") # QUEUED, RUNNING, SUCCESS, FAILED, UPSTREAM_FAILED, CANCELLED
    current_stage = Column(String, nullable=False, default="check_dataset")
    started_at = Column(DateTime, default=datetime.utcnow)
    completed_at = Column(DateTime, nullable=True)
    input_records = Column(Integer, default=0)
    bronze_records = Column(Integer, default=0)
    valid_records = Column(Integer, default=0)
    quarantined_records = Column(Integer, default=0)
    silver_records = Column(Integer, default=0)
    gold_records = Column(Integer, default=0)
    error_message = Column(Text, nullable=True)
    failed_stage = Column(String, nullable=True)
    databricks_executed = Column(Boolean, default=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

class DataQualityResultModel(Base):
    __tablename__ = "data_quality_results"

    id = Column(Integer, primary_key=True, autoincrement=True)
    run_id = Column(String, index=True, nullable=False)
    dataset_id = Column(String, index=True, nullable=False)
    batch_id = Column(String, index=True, nullable=False)
    check_name = Column(String, nullable=False)
    status = Column(String, nullable=False)  # PASS or FAIL
    records_checked = Column(Integer, default=0)
    records_failed = Column(Integer, default=0)
    failure_percentage = Column(Float, default=0.0)
    created_at = Column(DateTime, default=datetime.utcnow)

class QuarantineRecordModel(Base):
    __tablename__ = "quarantine_cloud_cost_records"

    id = Column(Integer, primary_key=True, autoincrement=True)
    run_id = Column(String, index=True, nullable=False)
    dataset_id = Column(String, index=True, nullable=False)
    batch_id = Column(String, index=True, nullable=False)
    record_hash = Column(String, index=True, nullable=True)
    failure_reason = Column(String, nullable=False)
    failed_at = Column(DateTime, default=datetime.utcnow)
    original_record = Column(Text, nullable=False)

class AirflowTaskInstanceModel(Base):
    __tablename__ = "airflow_task_instances"

    id = Column(Integer, primary_key=True, autoincrement=True)
    dag_id = Column(String, nullable=False, default="cloud_cost_etl_pipeline")
    dag_run_id = Column(String, index=True, nullable=False)
    dataset_id = Column(String, index=True, nullable=False)
    batch_id = Column(String, index=True, nullable=False)
    task_id = Column(String, index=True, nullable=False)
    status = Column(String, nullable=False, default="QUEUED")  # QUEUED, RUNNING, SUCCESS, FAILED, UPSTREAM_FAILED
    started_at = Column(DateTime, nullable=True)
    completed_at = Column(DateTime, nullable=True)
    duration_seconds = Column(Float, default=0.0)
    error_message = Column(Text, nullable=True)
    xcom_data = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)

class GoldCloudCostSummaryModel(Base):
    __tablename__ = "gold_cloud_cost_summary"

    id = Column(Integer, primary_key=True, autoincrement=True)
    dataset_id = Column(String, index=True, nullable=False)
    batch_id = Column(String, index=True, nullable=False)
    cloud_provider = Column(String, nullable=False)
    project_id = Column(String, nullable=False)
    region = Column(String, nullable=False)
    service = Column(String, nullable=False)
    environment = Column(String, nullable=False)
    month = Column(Integer, nullable=False)
    total_net_cost = Column(Float, default=0.0)
    total_list_cost = Column(Float, default=0.0)
    total_savings = Column(Float, default=0.0)
    avg_discount_pct = Column(Float, default=0.0)
    record_count = Column(Integer, default=0)
    high_risk_count = Column(Integer, default=0)
    created_at = Column(DateTime, default=datetime.utcnow)
