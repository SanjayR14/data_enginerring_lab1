import pytest
import os
import json
import time
from fastapi.testclient import TestClient
from backend.app.main import app
from backend.app.db.database import SessionLocal
from backend.app.models.dataset import DatasetModel
from backend.app.models.pipeline import PipelineRunModel, AirflowTaskInstanceModel, GoldCloudCostSummaryModel
from backend.app.services.airflow_service import AirflowOrchestratorService

client = TestClient(app)

def test_airflow_dag_parsing():
    """Verify that the cloud_cost_etl_pipeline Airflow DAG imports cleanly."""
    from airflow.dags.cloud_cost_pipeline import dag, TASK_SEQUENCE
    assert dag is not None
    assert dag.dag_id == "cloud_cost_etl_pipeline"
    assert len(TASK_SEQUENCE) == 12
    task_ids = [t[0] for t in TASK_SEQUENCE]
    expected_tasks = [
        "check_dataset", "validate_schema", "create_batch", "load_bronze",
        "bronze_quality_check", "clean_data", "feature_engineering",
        "silver_quality_check", "prepare_gold", "load_warehouse",
        "verify_result", "update_pipeline_status"
    ]
    assert task_ids == expected_tasks

def test_airflow_pipeline_trigger_and_execution():
    """Verify triggering Airflow DAG and asynchronous execution of all 12 tasks."""
    db = SessionLocal()
    try:
        # Create a test dataset entry
        dataset_id = "ds_airflow_test_001"
        db.query(DatasetModel).filter(DatasetModel.id == dataset_id).delete()
        db.query(PipelineRunModel).filter(PipelineRunModel.dataset_id == dataset_id).delete()
        db.commit()

        dataset = DatasetModel(
            id=dataset_id,
            filename=f"{dataset_id}.csv",
            original_filename="test_airflow_cloud_cost.csv",
            file_size=1024,
            file_type="csv",
            row_count=50,
            column_count=10,
            status="UPLOADED",
            storage_path=f"./data/uploads/{dataset_id}.csv",
            columns_json=json.dumps(["date", "cloud_provider", "account_id", "service", "net_cost"])
        )
        db.add(dataset)
        db.commit()

        # Trigger DAG execution via API endpoint
        response = client.post(f"/api/pipeline/process/{dataset_id}")
        assert response.status_code == 200
        data = response.json()
        assert data["dataset_id"] == dataset_id
        assert data["dag_id"] == "cloud_cost_etl_pipeline"
        assert "run_id" in data
        assert "dag_run_id" in data

        run_id = data["run_id"]

        # Wait for async execution thread to finish all 12 tasks
        time.sleep(3)

        # Check status endpoint
        status_resp = client.get(f"/api/pipeline/status/{dataset_id}")
        assert status_resp.status_code == 200
        status_data = status_resp.json()

        assert status_data["dataset_id"] == dataset_id
        assert status_data["status"] in ["SUCCESS", "COMPLETED", "PROCESSED_GOLD", "RUNNING"]
        assert len(status_data["tasks"]) == 12

        # Verify Gold layer creation
        gold_path = f"./data/delta/gold_cloud_cost_summary/{dataset_id}.parquet"
        assert os.path.exists(gold_path), f"Gold layer Parquet missing at {gold_path}"

        # Test Run Summary endpoint
        summary_resp = client.get(f"/api/pipeline/runs/{run_id}")
        assert summary_resp.status_code == 200
        summary_data = summary_resp.json()
        assert summary_data["run_id"] == run_id
        assert len(summary_data["tasks"]) == 12

    finally:
        db.close()
