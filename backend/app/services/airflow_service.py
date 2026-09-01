import os
import json
import logging
import requests
import threading
import time
from datetime import datetime
from typing import Dict, Any, List, Optional
from sqlalchemy.orm import Session

from backend.app.db.database import SessionLocal
from backend.app.core.config import settings
from backend.app.models.dataset import DatasetModel
from backend.app.models.pipeline import (
    PipelineRunModel,
    AirflowTaskInstanceModel,
    DataQualityResultModel,
    QuarantineRecordModel,
    GoldCloudCostSummaryModel
)
from backend.app.models.cdc import KafkaEventAuditModel

# Import tasks directly from DAG definition
from airflow.dags.cloud_cost_pipeline import (
    check_dataset,
    validate_schema,
    create_batch,
    load_bronze,
    bronze_quality_check,
    clean_data,
    feature_engineering,
    silver_quality_check,
    dimension_load,
    fact_load,
    warehouse_quality_check,
    refresh_olap_aggregates,
    warehouse_verification,
    update_pipeline_status
)

logger = logging.getLogger(__name__)

TASK_SEQUENCE = [
    ("check_dataset", check_dataset),
    ("validate_schema", validate_schema),
    ("create_batch", create_batch),
    ("load_bronze", load_bronze),
    ("bronze_quality_check", bronze_quality_check),
    ("clean_data", clean_data),
    ("feature_engineering", feature_engineering),
    ("silver_quality_check", silver_quality_check),
    ("dimension_load", dimension_load),
    ("fact_load", fact_load),
    ("warehouse_quality_check", warehouse_quality_check),
    ("refresh_olap_aggregates", refresh_olap_aggregates),
    ("warehouse_verification", warehouse_verification),
    ("update_pipeline_status", update_pipeline_status)
]

class MockTaskInstance:
    """Mock TaskInstance for XCom simulation during DAG execution."""
    def __init__(self):
        self._xcoms = {}

    def xcom_push(self, key='return_value', value=None):
        self._xcoms[key] = value

    def xcom_pull(self, task_ids=None, key='return_value'):
        return self._xcoms.get(task_ids, {}).get(key, {}) if isinstance(task_ids, str) else None


class AirflowOrchestratorService:

    @classmethod
    def trigger_dag_run(cls, dataset_id: str, batch_id: Optional[str] = None, db: Session = None) -> Dict[str, Any]:
        """
        Triggers the cloud_cost_etl_pipeline DAG for a dataset.
        1. Validates dataset exists in PostgreSQL.
        2. Generates batch_id and dag_run_id.
        3. Updates pipeline_runs tracking table.
        4. Triggers Airflow REST API if available & launches task execution loop.
        """
        close_db_on_exit = False
        if db is None:
            db = SessionLocal()
            close_db_on_exit = True

        try:
            dataset = db.query(DatasetModel).filter(DatasetModel.id == dataset_id).first()
            if not dataset:
                raise ValueError(f"Dataset '{dataset_id}' not found.")

            if not batch_id:
                batch_id = f"batch_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"

            timestamp_str = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S')
            dag_run_id = f"manual__{timestamp_str}"
            run_id = f"run_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}_{dataset_id[:8]}"

            # Check if there is an existing pipeline run for this dataset & batch (Idempotency)
            existing_run = db.query(PipelineRunModel).filter(
                PipelineRunModel.dataset_id == dataset_id,
                PipelineRunModel.batch_id == batch_id
            ).first()

            if existing_run and existing_run.status in ["RUNNING", "QUEUED"]:
                return {
                    "dataset_id": dataset_id,
                    "batch_id": batch_id,
                    "dag_id": "cloud_cost_etl_pipeline",
                    "dag_run_id": existing_run.dag_run_id or dag_run_id,
                    "run_id": existing_run.run_id,
                    "status": existing_run.status,
                    "message": "Pipeline run already active for this dataset & batch."
                }

            pipeline_run = PipelineRunModel(
                run_id=run_id,
                dataset_id=dataset_id,
                batch_id=batch_id,
                dag_run_id=dag_run_id,
                status="QUEUED",
                current_stage="check_dataset",
                started_at=datetime.utcnow(),
                input_records=dataset.row_count
            )
            db.add(pipeline_run)

            # Clear previous task instances for this dag_run_id
            db.query(AirflowTaskInstanceModel).filter(
                AirflowTaskInstanceModel.dag_run_id == dag_run_id
            ).delete()

            # Initialize 12 task instance records in QUEUED state
            for task_id, _ in TASK_SEQUENCE:
                ti = AirflowTaskInstanceModel(
                    dag_id="cloud_cost_etl_pipeline",
                    dag_run_id=dag_run_id,
                    dataset_id=dataset_id,
                    batch_id=batch_id,
                    task_id=task_id,
                    status="QUEUED"
                )
                db.add(ti)

            db.commit()

            # Prefer the real Airflow REST API. Only fall back to the in-process
            # execution engine if a real Airflow scheduler/webserver isn't reachable.
            real_airflow_triggered = False
            try:
                airflow_url = f"{settings.AIRFLOW_URL}/api/v1/dags/cloud_cost_etl_pipeline/dagRuns"
                auth = (settings.AIRFLOW_USERNAME, settings.AIRFLOW_PASSWORD)
                payload = {
                    "dag_run_id": dag_run_id,
                    "conf": {"dataset_id": dataset_id, "batch_id": batch_id}
                }
                resp = requests.post(airflow_url, json=payload, auth=auth, timeout=5)
                if resp.status_code in [200, 201]:
                    real_airflow_triggered = True
                    logger.info(f"[AIRFLOW REST] Successfully triggered real Airflow DAG run {dag_run_id} via REST API.")
                else:
                    logger.warning(f"[AIRFLOW REST] Airflow REST API returned {resp.status_code}: {resp.text[:200]}")
            except Exception as e:
                logger.info(f"[AIRFLOW REST] Real Airflow unreachable ({str(e)}). Falling back to in-process orchestrator engine.")

            if real_airflow_triggered:
                # Poll the real Airflow scheduler and mirror its task/DAG-run state
                # into our own tracking tables, instead of re-executing the pipeline
                # a second time in-process.
                thread = threading.Thread(
                    target=cls._poll_real_airflow_dag_run,
                    args=(dataset_id, batch_id, dag_run_id, run_id),
                    daemon=True
                )
                thread.start()
                orchestration_mode = "real_airflow"
            else:
                # Launch asynchronous thread to execute DAG task sequence with exact state tracking
                thread = threading.Thread(
                    target=cls._execute_dag_task_sequence,
                    args=(dataset_id, batch_id, dag_run_id, run_id),
                    daemon=True
                )
                thread.start()
                orchestration_mode = "in_process_fallback"

            return {
                "dataset_id": dataset_id,
                "batch_id": batch_id,
                "dag_id": "cloud_cost_etl_pipeline",
                "dag_run_id": dag_run_id,
                "run_id": run_id,
                "status": "QUEUED",
                "orchestration_mode": orchestration_mode,
                "message": "Airflow DAG 'cloud_cost_etl_pipeline' triggered successfully."
            }

        finally:
            if close_db_on_exit:
                db.close()

    @classmethod
    def _poll_real_airflow_dag_run(cls, dataset_id: str, batch_id: str, dag_run_id: str, run_id: str):
        """
        Polls a DAG run that was triggered on a real Apache Airflow scheduler/webserver
        via its REST API, and mirrors task instance states + XCom outputs into our own
        pipeline_runs / airflow_task_instances tracking tables so the existing
        /api/pipeline/status endpoint and React UI work unchanged against a real
        Airflow-driven run.
        """
        base_url = f"{settings.AIRFLOW_URL}/api/v1/dags/cloud_cost_etl_pipeline/dagRuns/{dag_run_id}"
        auth = (settings.AIRFLOW_USERNAME, settings.AIRFLOW_PASSWORD)

        # Map Airflow's task/dag-run states to this app's existing status vocabulary
        state_map = {
            "queued": "QUEUED", "scheduled": "QUEUED", "none": "QUEUED",
            "running": "RUNNING",
            "success": "SUCCESS",
            "failed": "FAILED",
            "upstream_failed": "UPSTREAM_FAILED",
            "skipped": "UPSTREAM_FAILED",
            "up_for_retry": "RUNNING",
            "up_for_reschedule": "RUNNING",
        }
        # Which task's XCom return value feeds which pipeline_run counter
        xcom_field_map = {
            "load_bronze": "bronze_records",
            "clean_data": "quarantined_records",
            "feature_engineering": "silver_records",
            "fact_load": "gold_records",
        }

        def _parse_ts(val):
            if not val:
                return None
            try:
                return datetime.fromisoformat(val.replace("Z", "+00:00")).replace(tzinfo=None)
            except Exception:
                return None

        db = SessionLocal()
        try:
            pipeline_run = db.query(PipelineRunModel).filter(PipelineRunModel.run_id == run_id).first()
            if pipeline_run:
                pipeline_run.status = "RUNNING"
                db.commit()

            terminal_states = {"success", "failed"}
            dag_run_state = "running"
            max_polls = 120  # up to ~10 minutes at 5s intervals
            for _ in range(max_polls):
                try:
                    dr_resp = requests.get(base_url, auth=auth, timeout=5)
                    dr_resp.raise_for_status()
                    dag_run_state = dr_resp.json().get("state", "running")

                    ti_resp = requests.get(f"{base_url}/taskInstances", auth=auth, timeout=5)
                    ti_resp.raise_for_status()
                    task_instances = ti_resp.json().get("task_instances", [])

                    for ti in task_instances:
                        task_id = ti["task_id"]
                        af_state = (ti.get("state") or "queued").lower()
                        mapped_state = state_map.get(af_state, "QUEUED")

                        ti_model = db.query(AirflowTaskInstanceModel).filter(
                            AirflowTaskInstanceModel.dag_run_id == dag_run_id,
                            AirflowTaskInstanceModel.task_id == task_id
                        ).first()
                        if ti_model:
                            ti_model.status = mapped_state
                            if ti.get("start_date"):
                                ti_model.started_at = _parse_ts(ti["start_date"])
                            if ti.get("end_date"):
                                ti_model.completed_at = _parse_ts(ti["end_date"])
                            if ti.get("duration") is not None:
                                ti_model.duration_seconds = round(ti["duration"], 2)
                            if af_state == "failed":
                                ti_model.error_message = "Task failed on Airflow scheduler (see Airflow logs)."

                        if pipeline_run:
                            pipeline_run.current_stage = task_id

                        # Pull XCom for tasks whose return value feeds our record counters
                        if mapped_state == "SUCCESS" and task_id in xcom_field_map and pipeline_run:
                            try:
                                xcom_resp = requests.get(
                                    f"{base_url}/taskInstances/{task_id}/xcomEntries/return_value",
                                    auth=auth, timeout=5
                                )
                                if xcom_resp.status_code == 200:
                                    xcom_val = xcom_resp.json().get("value")
                                    xcom_data = json.loads(xcom_val) if isinstance(xcom_val, str) else xcom_val
                                    if isinstance(xcom_data, dict):
                                        field = xcom_field_map[task_id]
                                        if field in xcom_data:
                                            setattr(pipeline_run, field, xcom_data[field])
                                        if "valid_records" in xcom_data:
                                            pipeline_run.valid_records = xcom_data["valid_records"]
                            except Exception as xe:
                                logger.warning(f"[AIRFLOW REST] Could not fetch XCom for {task_id}: {xe}")

                    db.commit()
                except Exception as poll_err:
                    logger.warning(f"[AIRFLOW REST] Poll error: {poll_err}")

                if dag_run_state in terminal_states:
                    break
                time.sleep(5)

            if pipeline_run:
                pipeline_run.status = state_map.get(dag_run_state, "FAILED")
                pipeline_run.completed_at = datetime.utcnow()
                if dag_run_state == "success":
                    pipeline_run.current_stage = "Pipeline Success (Gold Warehouse Ready) [Real Airflow]"
                else:
                    pipeline_run.current_stage = f"Pipeline {dag_run_state} [Real Airflow]"
                    pipeline_run.failed_stage = pipeline_run.current_stage
                db.commit()

            logger.info(f"[AIRFLOW REST] Finished polling real Airflow DAG run {dag_run_id}: final state {dag_run_state}")
        finally:
            db.close()

    @classmethod
    def _execute_dag_task_sequence(cls, dataset_id: str, batch_id: str, dag_run_id: str, run_id: str):
        """Asynchronous execution of the 12 Airflow DAG tasks with real-time state persistence."""
        db = SessionLocal()
        mock_ti = MockTaskInstance()
        context = {
            "params": {"dataset_id": dataset_id, "batch_id": batch_id},
            "ti": mock_ti
        }

        try:
            logger.info(f"[AIRFLOW DAG] Starting execution of 'cloud_cost_etl_pipeline' for run {dag_run_id}")
            pipeline_run = db.query(PipelineRunModel).filter(PipelineRunModel.run_id == run_id).first()
            if pipeline_run:
                pipeline_run.status = "RUNNING"
                db.commit()

            xcom_data_map = {}

            for task_id, task_func in TASK_SEQUENCE:
                start_time = datetime.utcnow()
                
                # 1. Update task instance status to RUNNING
                ti_model = db.query(AirflowTaskInstanceModel).filter(
                    AirflowTaskInstanceModel.dag_run_id == dag_run_id,
                    AirflowTaskInstanceModel.task_id == task_id
                ).first()
                
                if ti_model:
                    ti_model.status = "RUNNING"
                    ti_model.started_at = start_time
                    db.commit()

                # Update main pipeline run stage
                if pipeline_run:
                    pipeline_run.current_stage = task_id
                    db.commit()

                # 2. Execute task
                try:
                    res = task_func(**context)
                    end_time = datetime.utcnow()
                    duration = (end_time - start_time).total_seconds()

                    xcom_data_map[task_id] = res
                    mock_ti.xcom_push(key=task_id, value=res)

                    # Update task instance status to SUCCESS
                    if ti_model:
                        ti_model.status = "SUCCESS"
                        ti_model.completed_at = end_time
                        ti_model.duration_seconds = round(duration, 2)
                        ti_model.xcom_data = json.dumps(res) if res else None
                        db.commit()

                    # Update pipeline_run record counts from task outputs
                    if pipeline_run and res and isinstance(res, dict):
                        if "bronze_records" in res:
                            pipeline_run.bronze_records = res["bronze_records"]
                        if "valid_records" in res:
                            pipeline_run.valid_records = res["valid_records"]
                        if "quarantined_records" in res:
                            pipeline_run.quarantined_records = res["quarantined_records"]
                        if "silver_records" in res:
                            pipeline_run.silver_records = res["silver_records"]
                        if "gold_records" in res:
                            pipeline_run.gold_records = res["gold_records"]
                        db.commit()

                    time.sleep(0.1) # Small pause for realistic UI progress transition

                except Exception as task_err:
                    end_time = datetime.utcnow()
                    duration = (end_time - start_time).total_seconds()
                    err_msg = str(task_err)
                    logger.error(f"[AIRFLOW DAG] Task '{task_id}' FAILED: {err_msg}")

                    # Mark failed task instance
                    if ti_model:
                        ti_model.status = "FAILED"
                        ti_model.completed_at = end_time
                        ti_model.duration_seconds = round(duration, 2)
                        ti_model.error_message = err_msg
                        db.commit()

                    # Mark all downstream tasks as UPSTREAM_FAILED
                    remaining_tasks = [t[0] for t in TASK_SEQUENCE[TASK_SEQUENCE.index((task_id, task_func))+1:]]
                    for rem_id in remaining_tasks:
                        rem_ti = db.query(AirflowTaskInstanceModel).filter(
                            AirflowTaskInstanceModel.dag_run_id == dag_run_id,
                            AirflowTaskInstanceModel.task_id == rem_id
                        ).first()
                        if rem_ti:
                            rem_ti.status = "UPSTREAM_FAILED"
                            rem_ti.error_message = f"Skipped due to upstream failure in task '{task_id}'"
                            db.commit()

                    # Update main pipeline run status to FAILED
                    if pipeline_run:
                        pipeline_run.status = "FAILED"
                        pipeline_run.failed_stage = task_id
                        pipeline_run.error_message = f"Task '{task_id}' failed: {err_msg}"
                        pipeline_run.completed_at = end_time
                        db.commit()

                    return  # Stop downstream execution!

            # When all 12 tasks complete successfully
            end_time = datetime.utcnow()
            if pipeline_run:
                pipeline_run.status = "SUCCESS"
                pipeline_run.current_stage = "Pipeline Success (Gold Warehouse Ready)"
                pipeline_run.completed_at = end_time
                db.commit()

            dataset = db.query(DatasetModel).filter(DatasetModel.id == dataset_id).first()
            if dataset:
                dataset.status = "PROCESSED_GOLD"
                dataset.updated_at = end_time
                db.commit()

            logger.info(f"[AIRFLOW DAG] Successfully completed 'cloud_cost_etl_pipeline' for run {dag_run_id}")

        except Exception as global_err:
            logger.error(f"[AIRFLOW DAG] Global execution error: {str(global_err)}")
            pipeline_run = db.query(PipelineRunModel).filter(PipelineRunModel.run_id == run_id).first()
            if pipeline_run:
                pipeline_run.status = "FAILED"
                pipeline_run.error_message = str(global_err)
                pipeline_run.completed_at = datetime.utcnow()
                db.commit()
        finally:
            db.close()

    @classmethod
    def get_pipeline_status(cls, dataset_id: str, db: Session) -> Dict[str, Any]:
        """
        Returns full Airflow status and task breakdown for dataset_id.
        """
        run = db.query(PipelineRunModel).filter(
            PipelineRunModel.dataset_id == dataset_id
        ).order_by(PipelineRunModel.started_at.desc()).first()

        dataset = db.query(DatasetModel).filter(DatasetModel.id == dataset_id).first()
        
        # Calculate streaming CDC stats
        audit_events = db.query(KafkaEventAuditModel).filter(
            KafkaEventAuditModel.dataset_id == dataset_id
        ).all()
        kafka_events_count = len(audit_events)
        processed_count = sum(1 for e in audit_events if e.status in ["SUCCESS", "OUT_OF_ORDER_SKIPPED"])
        failed_count = sum(1 for e in audit_events if e.status in ["FAILED", "DLQ_INVALID"])
        duplicates_count = sum(1 for e in audit_events if e.status == "DUPLICATE_EVENT")

        if not run or not dataset:
            return {
                "run_id": None,
                "dag_run_id": None,
                "dataset_id": dataset_id,
                "batch_id": None,
                "status": "QUEUED" if dataset else "NOT_FOUND",
                "current_stage": "Awaiting Pipeline Trigger",
                "message": "Dataset uploaded. Click 'Process Dataset' to launch Airflow DAG pipeline.",
                "last_updated": dataset.updated_at if dataset else datetime.utcnow(),
                "tasks": [
                    {"task_id": tid, "status": "QUEUED", "started_at": None, "completed_at": None, "duration_seconds": 0.0, "error_message": None}
                    for tid, _ in TASK_SEQUENCE
                ],
                "input_records": dataset.row_count if dataset else 0,
                "bronze_records": 0,
                "valid_records": 0,
                "quarantined_records": 0,
                "silver_records": 0,
                "gold_records": 0,
                "failed_stage": None,
                "error_message": None,
                "databricks_executed": False,
                "kafka_events": kafka_events_count,
                "processed": processed_count,
                "failed": failed_count,
                "duplicates": duplicates_count
            }

        # Query task instances for dag_run_id
        task_instances = db.query(AirflowTaskInstanceModel).filter(
            AirflowTaskInstanceModel.dag_run_id == run.dag_run_id
        ).all() if run.dag_run_id else []

        ti_map = {t.task_id: t for t in task_instances}

        tasks_list = []
        for task_id, _ in TASK_SEQUENCE:
            ti = ti_map.get(task_id)
            tasks_list.append({
                "task_id": task_id,
                "status": ti.status if ti else "QUEUED",
                "started_at": ti.started_at if ti else None,
                "completed_at": ti.completed_at if ti else None,
                "duration_seconds": ti.duration_seconds if ti else 0.0,
                "error_message": ti.error_message if ti else None
            })

        msg = f"Airflow Pipeline status: {run.status}. Active stage: {run.current_stage}"
        if run.status == "SUCCESS":
            msg = f"Airflow DAG completed successfully. Published Gold analytical warehouse layer ({run.gold_records} summary records)."
        elif run.status == "FAILED":
            msg = f"Airflow DAG failed at task '{run.failed_stage}': {run.error_message}"

        return {
            "run_id": run.run_id,
            "dag_run_id": run.dag_run_id,
            "dataset_id": run.dataset_id,
            "batch_id": run.batch_id,
            "status": run.status,
            "current_stage": run.current_stage,
            "message": msg,
            "last_updated": run.completed_at or run.updated_at or run.started_at,
            "tasks": tasks_list,
            "input_records": run.input_records,
            "bronze_records": run.bronze_records,
            "valid_records": run.valid_records,
            "quarantined_records": run.quarantined_records,
            "silver_records": run.silver_records,
            "gold_records": run.gold_records,
            "failed_stage": run.failed_stage,
            "error_message": run.error_message,
            "databricks_executed": True,
            "kafka_events": kafka_events_count,
            "processed": processed_count,
            "failed": failed_count,
            "duplicates": duplicates_count
        }

    @classmethod
    def get_run_summary(cls, run_id: str, db: Session) -> Dict[str, Any]:
        """
        Returns safe summary of run info, failed stage, error details, and tasks.
        """
        run = db.query(PipelineRunModel).filter(
            (PipelineRunModel.run_id == run_id) | (PipelineRunModel.dag_run_id == run_id)
        ).first()

        if not run:
            raise ValueError(f"Run ID or DAG Run ID '{run_id}' not found.")

        task_instances = db.query(AirflowTaskInstanceModel).filter(
            AirflowTaskInstanceModel.dag_run_id == run.dag_run_id
        ).all()

        tasks_list = [
            {
                "task_id": t.task_id,
                "status": t.status,
                "started_at": t.started_at,
                "completed_at": t.completed_at,
                "duration_seconds": t.duration_seconds,
                "error_message": t.error_message
            }
            for t in task_instances
        ]

        return {
            "run_id": run.run_id,
            "dag_run_id": run.dag_run_id,
            "dataset_id": run.dataset_id,
            "batch_id": run.batch_id,
            "status": run.status,
            "current_stage": run.current_stage,
            "failed_stage": run.failed_stage,
            "error_summary": run.error_message,
            "started_at": run.started_at,
            "completed_at": run.completed_at,
            "records_processed": run.silver_records,
            "tasks": tasks_list
        }
