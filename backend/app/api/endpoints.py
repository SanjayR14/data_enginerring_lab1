import json
import os
from typing import List, Optional, Dict, Any
from datetime import datetime
import pandas as pd
from fastapi import APIRouter, Depends, UploadFile, File, HTTPException, status, Query, BackgroundTasks
from fastapi.responses import FileResponse
from sqlalchemy.orm import Session

from backend.app.db.database import get_db
from backend.app.services.dataset_service import DatasetService
from backend.app.services.etl_service import ETLPipelineService
from backend.app.services.airflow_service import AirflowOrchestratorService
from backend.app.models.pipeline import PipelineRunModel, DataQualityResultModel, QuarantineRecordModel, AirflowTaskInstanceModel, GoldCloudCostSummaryModel
from backend.app.schemas.dataset import (
    DatasetResponse,
    DatasetDetailResponse,
    DatasetPreviewResponse,
    ColumnDetail
)
from backend.app.schemas.pipeline import (
    PipelineProcessResponse,
    PipelineStatusResponse,
    PipelineRunDetailResponse,
    DataQualitySummaryResponse,
    DataQualityCheckItem,
    QuarantineRecordItem,
    DatasetProfileResponse
)
from backend.app.schemas.cdc import (
    CloudCostEventRequest,
    CloudCostBatchEventRequest,
    EventStatusResponse,
    KafkaStatusResponse,
    KafkaMetricsResponse,
    CdcRecordHistoryResponse
)
from backend.app.models.cdc import KafkaEventAuditModel, PipelineEventMetricsModel
from backend.app.services.kafka_producer import KafkaProducerService
from backend.app.services.kafka_consumer import KafkaConsumerService
from backend.app.services.cdc_service import CDCService
from backend.app.core.config import settings

router = APIRouter()

@router.get("/health")
def health_check(db: Session = Depends(get_db)):
    try:
        from sqlalchemy import text
        db.execute(text("SELECT 1"))
        db_status = "connected"
    except Exception as e:
        db_status = f"error: {str(e)}"

    return {
        "status": "healthy",
        "database": db_status,
        "storage": "ready",
        "timestamp": datetime.utcnow().isoformat()
    }

@router.post("/datasets/upload", response_model=DatasetResponse, status_code=status.HTTP_201_CREATED)
async def upload_dataset(
    file: UploadFile = File(...),
    db: Session = Depends(get_db)
):
    try:
        content = await file.read()
        dataset, is_duplicate = DatasetService.validate_and_save_csv(file, content, db)

        # Publish event to Kafka
        if not is_duplicate:
            from backend.app.services.kafka_producer import KafkaProducerService
            import uuid
            import logging
            
            event = {
                "dataset_id": dataset.id,
                "batch_id": f"batch_{uuid.uuid4().hex[:8]}",
                "file_path": dataset.storage_path,
                "row_count": dataset.row_count,
            }
            try:
                KafkaProducerService.publish_event(event)
                logging.getLogger("kafka_producer").info(f"Published dataset upload event to Kafka: {dataset.id}")
            except Exception as e:
                logging.getLogger("kafka_producer").error(f"Failed to publish to Kafka: {e}")
                
            # Automatically trigger the Airflow DAG for the new dataset
            from backend.app.services.airflow_service import AirflowOrchestratorService
            try:
                AirflowOrchestratorService.trigger_dag_run(dataset_id=dataset.id, db=db)
                logging.getLogger("airflow_service").info(f"Successfully triggered Airflow DAG for dataset {dataset.id}")
            except Exception as e:
                logging.getLogger("airflow_service").error(f"Failed to trigger Airflow DAG for dataset {dataset.id}: {e}")

        cols = []
        if dataset.columns_json:
            try:
                data = json.loads(dataset.columns_json)
                cols = data.get("columns", [])
            except Exception:
                pass

        return DatasetResponse(
            id=dataset.id,
            filename=dataset.filename,
            original_filename=dataset.original_filename,
            file_size=dataset.file_size,
            file_type=dataset.file_type,
            row_count=dataset.row_count,
            column_count=dataset.column_count,
            status=dataset.status,
            upload_timestamp=dataset.upload_timestamp,
            file_hash=dataset.file_hash,
            columns=cols
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An unexpected error occurred during upload: {str(e)}"
        )

@router.get("/datasets", response_model=List[DatasetResponse])
def list_datasets(db: Session = Depends(get_db)):
    datasets = DatasetService.list_datasets(db)
    result = []
    for d in datasets:
        cols = []
        if d.columns_json:
            try:
                data = json.loads(d.columns_json)
                cols = data.get("columns", [])
            except Exception:
                pass
        result.append(
            DatasetResponse(
                id=d.id,
                filename=d.filename,
                original_filename=d.original_filename,
                file_size=d.file_size,
                file_type=d.file_type,
                row_count=d.row_count,
                column_count=d.column_count,
                status=d.status,
                upload_timestamp=d.upload_timestamp,
                file_hash=d.file_hash,
                columns=cols
            )
        )
    return result

@router.get("/datasets/sample/download")
def download_sample_dataset():
    sample_path = "./data/sample/cloud_cost_dataset.csv"
    if not os.path.exists(sample_path):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Sample dataset is missing on the server."
        )
    return FileResponse(
        path=sample_path,
        filename="cloud_cost_sample_dataset.csv",
        media_type="text/csv"
    )

@router.get("/datasets/{dataset_id}", response_model=DatasetDetailResponse)
def get_dataset(dataset_id: str, db: Session = Depends(get_db)):
    dataset = DatasetService.get_dataset(dataset_id, db)
    
    cols = []
    col_details = []
    if dataset.columns_json:
        try:
            data = json.loads(dataset.columns_json)
            cols = data.get("columns", [])
            types = data.get("types", {})
            for c in cols:
                col_details.append(ColumnDetail(name=c, data_type=types.get(c, "unknown")))
        except Exception:
            pass

    return DatasetDetailResponse(
        id=dataset.id,
        filename=dataset.filename,
        original_filename=dataset.original_filename,
        file_size=dataset.file_size,
        file_type=dataset.file_type,
        row_count=dataset.row_count,
        column_count=dataset.column_count,
        status=dataset.status,
        upload_timestamp=dataset.upload_timestamp,
        file_hash=dataset.file_hash,
        columns=cols,
        storage_path=dataset.storage_path,
        column_details=col_details
    )

@router.get("/datasets/{dataset_id}/preview", response_model=DatasetPreviewResponse)
def get_dataset_preview(
    dataset_id: str,
    layer: str = Query("bronze", description="Layer to preview: 'bronze' or 'silver'"),
    limit: int = 10,
    db: Session = Depends(get_db)
):
    dataset = DatasetService.get_dataset(dataset_id, db)

    # If silver preview requested and parquet exists, read silver parquet
    silver_parquet = f"./data/delta/silver_cloud_cost_clean/{dataset_id}.parquet"
    if layer == "silver" and os.path.exists(silver_parquet):
        try:
            df = pd.read_parquet(silver_parquet).head(limit)
            df = df.where(pd.notnull(df), None)
            columns = [str(c) for c in df.columns]
            column_types = {str(col): str(dtype) for col, dtype in df.dtypes.items()}
            return DatasetPreviewResponse(
                dataset_id=dataset.id,
                original_filename=f"{dataset.original_filename} (Silver Cleaned & Feature Engineered)",
                row_count=dataset.row_count,
                column_count=len(columns),
                columns=columns,
                column_types=column_types,
                preview_data=df.to_dict(orient="records")
            )
        except Exception as e:
            pass # Fallback to raw dataset preview if error

    return DatasetService.get_dataset_preview(dataset_id, db, limit=limit)

# -------------------------------------------------------------------
# PHASE 4: AIRFLOW ORCHESTRATION & PIPELINE ENDPOINTS
# -------------------------------------------------------------------

@router.post("/pipeline/process/{dataset_id}", response_model=PipelineProcessResponse)
def trigger_pipeline_process(
    dataset_id: str,
    db: Session = Depends(get_db)
):
    """
    Triggers Phase 4 Apache Airflow Orchestrated Pipeline execution for the dataset.
    Executes the 12 Airflow tasks: check_dataset -> validate_schema -> create_batch -> load_bronze ->
    bronze_quality_check -> clean_data -> feature_engineering -> silver_quality_check ->
    prepare_gold -> load_warehouse -> verify_result -> update_pipeline_status
    """
    try:
        res = AirflowOrchestratorService.trigger_dag_run(dataset_id=dataset_id, db=db)
        return PipelineProcessResponse(
            run_id=res["run_id"],
            dataset_id=res["dataset_id"],
            batch_id=res["batch_id"],
            dag_id=res["dag_id"],
            dag_run_id=res["dag_run_id"],
            status=res["status"],
            message=res["message"],
            started_at=datetime.utcnow()
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Airflow DAG trigger failed: {str(e)}"
        )

@router.get("/pipeline/status/{dataset_id}", response_model=PipelineStatusResponse)
def get_pipeline_status(dataset_id: str, db: Session = Depends(get_db)):
    """
    Returns real-time Airflow DAG execution status, 12-task breakdown, and CDC event metrics.
    """
    try:
        status_data = AirflowOrchestratorService.get_pipeline_status(dataset_id=dataset_id, db=db)
        return PipelineStatusResponse(**status_data)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to fetch Airflow pipeline status: {str(e)}"
        )

@router.get("/pipeline/runs/{run_id}", response_model=PipelineRunDetailResponse)
def get_pipeline_run_detail(run_id: str, db: Session = Depends(get_db)):
    """
    Returns Airflow run summary including failed stage, error summary, timestamps, and task instances.
    """
    try:
        summary = AirflowOrchestratorService.get_run_summary(run_id=run_id, db=db)
        return PipelineRunDetailResponse(**summary)
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to fetch run summary: {str(e)}"
        )

@router.get("/datasets/{dataset_id}/data-quality", response_model=DataQualitySummaryResponse)
def get_data_quality_summary(dataset_id: str, db: Session = Depends(get_db)):
    """
    Returns Data Quality checks summary and score.
    """
    run = db.query(PipelineRunModel).filter(
        PipelineRunModel.dataset_id == dataset_id
    ).order_by(PipelineRunModel.started_at.desc()).first()

    if not run:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Pipeline execution has not been run for this dataset yet."
        )

    checks = db.query(DataQualityResultModel).filter(
        DataQualityResultModel.run_id == run.run_id
    ).all()

    check_items = [
        DataQualityCheckItem(
            check_name=c.check_name,
            status=c.status,
            records_checked=c.records_checked,
            records_failed=c.records_failed,
            failure_percentage=c.failure_percentage
        )
        for c in checks
    ]

    total = run.input_records or 1
    valid = run.valid_records or 0
    quarantined = run.quarantined_records or 0
    score = round((valid / total) * 100.0, 1)

    # Count null & invalid check failures
    null_issues = sum(c.records_failed for c in checks if "Null" in c.check_name)
    invalid_vals = sum(c.records_failed for c in checks if "Range" in c.check_name or "Numeric" in c.check_name or "Date" in c.check_name)
    duplicate_recs = sum(c.records_failed for c in checks if "Duplicate" in c.check_name)

    return DataQualitySummaryResponse(
        dataset_id=dataset_id,
        batch_id=run.batch_id,
        total_records=total,
        valid_records=valid,
        quarantined_records=quarantined,
        duplicate_records=duplicate_recs,
        null_issues=null_issues,
        invalid_values=invalid_vals,
        quality_score=score,
        checks=check_items
    )

@router.get("/datasets/{dataset_id}/quarantine", response_model=List[QuarantineRecordItem])
def get_quarantine_records(dataset_id: str, limit: int = 50, db: Session = Depends(get_db)):
    """
    Returns quarantined invalid records for dataset inspection.
    """
    records = db.query(QuarantineRecordModel).filter(
        QuarantineRecordModel.dataset_id == dataset_id
    ).order_by(QuarantineRecordModel.failed_at.desc()).limit(limit).all()

    return [
        QuarantineRecordItem(
            id=r.id,
            record_hash=r.record_hash,
            failure_reason=r.failure_reason,
            failed_at=r.failed_at,
            original_record=r.original_record
        )
        for r in records
    ]

@router.get("/datasets/{dataset_id}/profile", response_model=DatasetProfileResponse)
def get_dataset_profile(dataset_id: str, db: Session = Depends(get_db)):
    """
    Returns dataset profiling metrics (EDA preparation).
    """
    try:
        profile = ETLPipelineService.get_dataset_profile(dataset_id, db)
        return DatasetProfileResponse(**profile)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to calculate dataset profile: {str(e)}"
        )

# -------------------------------------------------------------------
# PHASE 3: KAFKA STREAMING & CDC ENDPOINTS
# -------------------------------------------------------------------

@router.post("/events/cloud-cost", status_code=status.HTTP_201_CREATED)
def publish_cloud_cost_event(
    event_req: CloudCostEventRequest,
    db: Session = Depends(get_db)
):
    """
    Publishes one streaming cloud cost event (INSERT, UPDATE, DELETE) to Kafka topic cloud-cost-events.
    """
    try:
        payload = event_req.model_dump()
        pub_res = KafkaProducerService.publish_event(payload)
        return pub_res
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to publish streaming event: {str(e)}"
        )

@router.post("/events/cloud-cost/batch", status_code=status.HTTP_201_CREATED)
def publish_cloud_cost_batch(
    batch_req: CloudCostBatchEventRequest,
    db: Session = Depends(get_db)
):
    """
    Publishes a batch of streaming cloud cost events to Kafka.
    """
    try:
        events = [e.model_dump() for e in batch_req.events]
        res = KafkaProducerService.publish_batch(events)
        return res
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to publish batch events: {str(e)}"
        )

@router.get("/events/{event_id}", response_model=EventStatusResponse)
def get_event_status(event_id: str, db: Session = Depends(get_db)):
    """
    Retrieves processing status and audit log for a specific Kafka event.
    """
    audit = db.query(KafkaEventAuditModel).filter(
        KafkaEventAuditModel.event_id == event_id
    ).first()

    if not audit:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Event ID '{event_id}' not found in kafka_event_audit log."
        )

    return EventStatusResponse(
        event_id=audit.event_id,
        dataset_id=audit.dataset_id,
        batch_id=audit.batch_id,
        business_key=audit.business_key,
        operation=audit.operation,
        event_timestamp=audit.event_timestamp,
        received_at=audit.received_at,
        processed_at=audit.processed_at,
        status=audit.status,
        error_message=audit.error_message
    )

@router.get("/kafka/status", response_model=KafkaStatusResponse)
def get_kafka_status():
    """
    Returns Kafka connectivity, topics, and consumer status.
    """
    producer = KafkaProducerService.get_producer()
    is_connected = producer is not None
    return KafkaStatusResponse(
        status="ACTIVE" if is_connected else "ACTIVE (LOCAL FALLBACK BUFFER)",
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
        topic=settings.KAFKA_TOPIC,
        consumer_group=settings.KAFKA_GROUP_ID,
        dlq_topic=settings.KAFKA_DLQ_TOPIC,
        kafka_connected=is_connected,
        active_consumers=1
    )

@router.get("/kafka/metrics", response_model=KafkaMetricsResponse)
def get_kafka_metrics(db: Session = Depends(get_db)):
    """
    Returns aggregated Kafka streaming metrics.
    """
    m = db.query(PipelineEventMetricsModel).first()
    if not m:
        m = PipelineEventMetricsModel()

    return KafkaMetricsResponse(
        received_count=m.received_count or 0,
        processed_count=m.processed_count or 0,
        success_count=m.success_count or 0,
        failed_count=m.failed_count or 0,
        duplicate_count=m.duplicate_count or 0,
        dlq_count=m.dlq_count or 0,
        insert_count=m.insert_count or 0,
        update_count=m.update_count or 0,
        delete_count=m.delete_count or 0,
        avg_processing_time_ms=m.avg_processing_time_ms or 0.0,
        last_event_timestamp=m.last_event_timestamp
    )

@router.get("/cdc/records/{business_key:path}", response_model=CdcRecordHistoryResponse)
def get_cdc_record_history(
    business_key: str,
    dataset_id: str = "ds_sample_test"
):
    """
    Retrieves full CDC event lineage and current Delta Lake state for a business key.
    """
    res = CDCService.get_cdc_history(business_key, dataset_id=dataset_id)
    return CdcRecordHistoryResponse(**res)

@router.get("/cdc/audit", response_model=List[EventStatusResponse])
def get_cdc_audit_logs(
    limit: int = 50,
    status_filter: Optional[str] = None,
    db: Session = Depends(get_db)
):
    """
    Retrieves recent Kafka event audit entries.
    """
    query = db.query(KafkaEventAuditModel)
    if status_filter:
        query = query.filter(KafkaEventAuditModel.status == status_filter.upper())

    audits = query.order_by(KafkaEventAuditModel.received_at.desc()).limit(limit).all()

    return [
        EventStatusResponse(
            event_id=a.event_id,
            dataset_id=a.dataset_id,
            batch_id=a.batch_id,
            business_key=a.business_key,
            operation=a.operation,
            event_timestamp=a.event_timestamp,
            received_at=a.received_at,
            processed_at=a.processed_at,
            status=a.status,
            error_message=a.error_message
        )
        for a in audits
    ]

# -------------------------------------------------------------------
# PHASE 5: DATA WAREHOUSE & ANALYTICAL ENDPOINTS
# -------------------------------------------------------------------

@router.get("/warehouse/summary")
def get_warehouse_summary():
    """
    Returns summary statistics, catalog namespace, and table row counts from the Data Warehouse.
    """
    from backend.app.services.warehouse_service import DataWarehouseEngine
    return DataWarehouseEngine.get_warehouse_summary()

@router.get("/warehouse/queries")
def get_warehouse_analytical_queries():
    """
    Executes and returns results for all 15 warehouse analytical queries.
    """
    from backend.app.services.warehouse_service import DataWarehouseEngine
    return DataWarehouseEngine.execute_analytical_queries()

@router.get("/warehouse/providers")
def get_warehouse_provider_costs():
    from backend.app.services.warehouse_service import DataWarehouseEngine
    queries = DataWarehouseEngine.execute_analytical_queries()
    provider_q = next((q for q in queries if q['id'] == 2), None)
    return {"catalog": "cloud_cost_catalog.cloud_warehouse", "data": provider_q['result'] if provider_q else []}

@router.get("/warehouse/departments")
def get_warehouse_department_costs():
    from backend.app.services.warehouse_service import DataWarehouseEngine
    queries = DataWarehouseEngine.execute_analytical_queries()
    dept_q = next((q for q in queries if q['id'] == 4), None)
    return {"catalog": "cloud_cost_catalog.cloud_warehouse", "data": dept_q['result'] if dept_q else []}

@router.get("/warehouse/services")
def get_warehouse_service_costs():
    from backend.app.services.warehouse_service import DataWarehouseEngine
    queries = DataWarehouseEngine.execute_analytical_queries()
    svc_q = next((q for q in queries if q['id'] == 5), None)
    return {"catalog": "cloud_cost_catalog.cloud_warehouse", "data": svc_q['result'] if svc_q else []}

@router.get("/warehouse/monthly-cost")
def get_warehouse_monthly_costs():
    from backend.app.services.warehouse_service import DataWarehouseEngine
    queries = DataWarehouseEngine.execute_analytical_queries()
    m_q = next((q for q in queries if q['id'] == 3), None)
    return {"catalog": "cloud_cost_catalog.cloud_warehouse", "data": m_q['result'] if m_q else []}

@router.get("/warehouse/budget")
def get_warehouse_budget_analysis():
    from backend.app.services.warehouse_service import DataWarehouseEngine
    queries = DataWarehouseEngine.execute_analytical_queries()
    b_q = next((q for q in queries if q['id'] == 8), None)
    return {"catalog": "cloud_cost_catalog.cloud_warehouse", "data": b_q['result'] if b_q else []}

@router.get("/warehouse/anomalies")
def get_warehouse_anomalies():
    from backend.app.services.warehouse_service import DataWarehouseEngine
    queries = DataWarehouseEngine.execute_analytical_queries()
    a_q = next((q for q in queries if q['id'] == 11), None)
    return {"catalog": "cloud_cost_catalog.cloud_warehouse", "data": a_q['result'] if a_q else []}

@router.get("/warehouse/schema")
def get_warehouse_schema():
    return {
        "catalog": "cloud_cost_catalog",
        "schema": "cloud_warehouse",
        "fact_table": {
            "name": "fact_cloud_cost",
            "grain": "ONE ROW PER DATE, ACCOUNT, PROJECT, ENVIRONMENT, PROVIDER, REGION, SERVICE, AND RESOURCE TYPE",
            "primary_key": "fact_id",
            "foreign_keys": [
                "date_key", "cloud_key", "account_key", "project_key",
                "organization_key", "location_key", "service_key", "environment_key", "currency_key"
            ],
            "measures": [
                "usage_quantity", "list_cost", "discount_amount", "net_cost",
                "reserved_savings", "savings_plan_savings", "spot_savings", "total_savings",
                "budget_amount", "budget_remaining", "budget_utilization_pct", "effective_discount_pct"
            ]
        },
        "star_dimensions": [
            "dim_date", "dim_cloud", "dim_account", "dim_project",
            "dim_organization", "dim_location", "dim_service", "dim_environment", "dim_currency"
        ],
        "snowflake_dimensions": [
            "dim_business_unit", "dim_department", "dim_cost_center",
            "dim_cloud", "dim_account", "dim_project"
        ]
    }

@router.post("/warehouse/scd2-demo")
def trigger_scd2_demo(
    project_id: str = Query("prj-analytics", description="Project ID to update"),
    new_environment: str = Query("staging", description="New environment state")
):
    """
    Simulates Slowly Changing Dimension (SCD Type 2) record creation.
    """
    from backend.app.services.warehouse_service import DataWarehouseEngine
    return DataWarehouseEngine.simulate_scd2_update(project_id, new_environment)


# =============================================================================
# PHASE 6: OLAP, DATA CUBE & MULTIDIMENSIONAL ANALYTICAL ENDPOINTS
# =============================================================================

@router.get("/olap/metadata")
def get_olap_metadata():
    from backend.app.services.olap_service import OLAPEngine
    return OLAPEngine.get_metadata()


@router.get("/olap/cube")
def get_olap_cube_summary():
    from backend.app.services.olap_service import OLAPEngine
    df = OLAPEngine.get_cube_dataset()
    return {
        "cube_name": "cloud_cost_cube",
        "record_count": len(df),
        "total_net_cost": round(float(df['net_cost'].sum()), 2),
        "total_budget": round(float(df['budget_amount'].sum()), 2),
        "dimensions_available": [
            "date", "cloud_provider", "account_id", "project_id",
            "business_unit", "department", "cost_center", "region",
            "service", "resource_type", "environment", "currency"
        ]
    }


@router.get("/olap/rollup")
def olap_rollup(
    dimension: str = Query("time", description="Dimension to rollup"),
    level: str = Query("month", description="Target level"),
    measure: str = Query("net_cost", description="Target measure"),
    provider: Optional[str] = Query(None),
    department: Optional[str] = Query(None),
    environment: Optional[str] = Query(None),
    year: Optional[int] = Query(None)
):
    from backend.app.services.olap_service import OLAPEngine
    try:
        filters = {}
        if provider: filters["cloud_provider"] = provider
        if department: filters["department"] = department
        if environment: filters["environment"] = environment
        if year: filters["year"] = year

        return OLAPEngine.rollup(dimension=dimension, level=level, measure=measure, filters=filters)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/olap/drilldown")
def olap_drilldown(
    hierarchy: str = Query("time", description="Hierarchy name"),
    current_level: str = Query("year", description="Current parent level"),
    next_level: str = Query("quarter", description="Next child level"),
    measure: str = Query("net_cost", description="Target measure"),
    provider: Optional[str] = Query(None),
    department: Optional[str] = Query(None),
    environment: Optional[str] = Query(None),
    year: Optional[int] = Query(None)
):
    from backend.app.services.olap_service import OLAPEngine
    try:
        filters = {}
        if provider: filters["cloud_provider"] = provider
        if department: filters["department"] = department
        if environment: filters["environment"] = environment
        if year: filters["year"] = year

        return OLAPEngine.drilldown(
            hierarchy=hierarchy, current_level=current_level, next_level=next_level,
            filters=filters, measure=measure
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/olap/slice")
def olap_slice(
    dimension: str = Query("cloud_provider", description="Dimension to slice"),
    value: str = Query("AWS", description="Slice fixed value"),
    measure: str = Query("net_cost", description="Target measure")
):
    from backend.app.services.olap_service import OLAPEngine
    try:
        return OLAPEngine.slice(dimension=dimension, value=value, measure=measure)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/olap/dice")
@router.post("/olap/dice")
def olap_dice(
    payload: Optional[Dict[str, Any]] = None,
    provider: Optional[str] = Query(None),
    department: Optional[str] = Query(None),
    environment: Optional[str] = Query(None)
):
    from backend.app.services.olap_service import OLAPEngine
    filters = {}
    measures = ["net_cost", "budget_amount", "total_savings"]

    if payload and "filters" in payload:
        filters = payload.get("filters", {})
        if "measures" in payload:
            measures = payload.get("measures", measures)
    else:
        if provider and provider != "All": filters["cloud_provider"] = provider.split(",") if "," in provider else provider
        if department and department != "All": filters["department"] = department.split(",") if "," in department else department
        if environment and environment != "All": filters["environment"] = environment.split(",") if "," in environment else environment

    return OLAPEngine.dice(filters=filters, measures=measures)


@router.get("/olap/pivot")
def olap_pivot(
    rows: str = Query("department", description="Rows dimension"),
    columns: str = Query("cloud_provider", description="Columns dimension"),
    measure: str = Query("net_cost", description="Target measure"),
    provider: Optional[str] = Query(None),
    environment: Optional[str] = Query(None),
    year: Optional[int] = Query(None)
):
    from backend.app.services.olap_service import OLAPEngine
    try:
        filters = {}
        if provider: filters["cloud_provider"] = provider
        if environment: filters["environment"] = environment
        if year: filters["year"] = year

        return OLAPEngine.pivot(rows=rows, columns=columns, measure=measure, filters=filters)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/olap/top")
def olap_top(
    category: str = Query("projects", description="Category: projects, services, departments, regions, accounts, anomalies"),
    n: int = Query(10, description="Top N limit"),
    measure: str = Query("net_cost", description="Target measure")
):
    from backend.app.services.olap_service import OLAPEngine
    try:
        return OLAPEngine.get_top_n(category=category, n=n, measure=measure)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/olap/time-series")
def olap_time_series(
    granularity: str = Query("monthly", description="Granularity: daily, weekly, monthly, quarterly, yearly"),
    measure: str = Query("net_cost", description="Target measure"),
    provider: Optional[str] = Query(None),
    department: Optional[str] = Query(None)
):
    from backend.app.services.olap_service import OLAPEngine
    try:
        filters = {}
        if provider: filters["cloud_provider"] = provider
        if department: filters["department"] = department

        return OLAPEngine.get_time_series(granularity=granularity, measure=measure, filters=filters)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/olap/budget")
def olap_budget_analysis(
    provider: Optional[str] = Query(None),
    department: Optional[str] = Query(None),
    year: Optional[int] = Query(None)
):
    from backend.app.services.olap_service import OLAPEngine
    filters = {}
    if provider: filters["cloud_provider"] = provider
    if department: filters["department"] = department
    if year: filters["year"] = year

    return OLAPEngine.get_budget_analysis(filters=filters)


@router.get("/olap/savings")
def olap_savings_analysis(
    provider: Optional[str] = Query(None),
    department: Optional[str] = Query(None),
    year: Optional[int] = Query(None)
):
    from backend.app.services.olap_service import OLAPEngine
    filters = {}
    if provider: filters["cloud_provider"] = provider
    if department: filters["department"] = department
    if year: filters["year"] = year

    return OLAPEngine.get_savings_analysis(filters=filters)


@router.get("/olap/anomalies")
def olap_anomaly_analysis(
    provider: Optional[str] = Query(None),
    department: Optional[str] = Query(None),
    year: Optional[int] = Query(None)
):
    from backend.app.services.olap_service import OLAPEngine
    filters = {}
    if provider: filters["cloud_provider"] = provider
    if department: filters["department"] = department
    if year: filters["year"] = year

    return OLAPEngine.get_anomaly_analysis(filters=filters)


@router.get("/olap/compare")
def olap_comparison(
    dimension: str = Query("cloud_provider", description="Dimension to compare"),
    values: Optional[str] = Query(None, description="Comma-separated values to compare"),
    measure: str = Query("net_cost", description="Target measure")
):
    from backend.app.services.olap_service import OLAPEngine
    try:
        val_list = [v.strip() for v in values.split(",")] if values else None
        return OLAPEngine.get_comparison_analysis(dimension=dimension, values=val_list, measure=measure)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


