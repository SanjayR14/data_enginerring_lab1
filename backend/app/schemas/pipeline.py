from pydantic import BaseModel
from typing import List, Dict, Any, Optional
from datetime import datetime

class AirflowTaskStatusItem(BaseModel):
    task_id: str
    status: str  # QUEUED, RUNNING, SUCCESS, FAILED, UPSTREAM_FAILED, SKIPPED
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    duration_seconds: float = 0.0
    error_message: Optional[str] = None

class PipelineProcessResponse(BaseModel):
    run_id: str
    dataset_id: str
    batch_id: str
    dag_id: str = "cloud_cost_etl_pipeline"
    dag_run_id: str
    status: str
    message: str
    started_at: datetime

class PipelineStatusResponse(BaseModel):
    run_id: Optional[str] = None
    dag_run_id: Optional[str] = None
    dataset_id: str
    batch_id: Optional[str] = None
    status: str
    current_stage: str
    message: str
    last_updated: datetime
    tasks: List[AirflowTaskStatusItem] = []
    input_records: int = 0
    bronze_records: int = 0
    valid_records: int = 0
    quarantined_records: int = 0
    silver_records: int = 0
    gold_records: int = 0
    failed_stage: Optional[str] = None
    error_message: Optional[str] = None
    databricks_executed: bool = False
    kafka_events: int = 0
    processed: int = 0
    failed: int = 0
    duplicates: int = 0

class PipelineRunDetailResponse(BaseModel):
    run_id: str
    dag_run_id: Optional[str] = None
    dataset_id: str
    batch_id: str
    status: str
    current_stage: str
    failed_stage: Optional[str] = None
    error_summary: Optional[str] = None
    started_at: datetime
    completed_at: Optional[datetime] = None
    records_processed: int = 0
    tasks: List[AirflowTaskStatusItem] = []

class DataQualityCheckItem(BaseModel):
    check_name: str
    status: str
    records_checked: int
    records_failed: int
    failure_percentage: float

class DataQualitySummaryResponse(BaseModel):
    dataset_id: str
    batch_id: str
    total_records: int
    valid_records: int
    quarantined_records: int
    duplicate_records: int
    null_issues: int
    invalid_values: int
    quality_score: float
    checks: List[DataQualityCheckItem]

class QuarantineRecordItem(BaseModel):
    id: int
    record_hash: Optional[str]
    failure_reason: str
    failed_at: datetime
    original_record: str

class DatasetProfileResponse(BaseModel):
    dataset_id: str
    row_count: int
    column_count: int
    null_counts: Dict[str, int]
    distinct_counts: Dict[str, int]
    numeric_stats: Dict[str, Dict[str, Optional[float]]]
    categorical_frequencies: Dict[str, Dict[str, int]]
    duplicate_count: int = 0
    correlation_matrix: Dict[str, Dict[str, Optional[float]]] = {}
    outliers: Dict[str, Dict[str, Any]] = {}
