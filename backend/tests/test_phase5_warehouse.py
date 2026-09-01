import pytest
import os
from fastapi.testclient import TestClient
from backend.app.main import app
from backend.app.services.warehouse_service import DataWarehouseEngine

client = TestClient(app)

def test_warehouse_load_and_summary():
    # Load warehouse from default sample data
    res = DataWarehouseEngine.load_warehouse_from_silver("ds_sample_test", "batch-001")
    assert res["status"] == "SUCCESS"
    assert res["catalog"] == "cloud_cost_catalog"
    assert res["schema"] == "cloud_warehouse"
    assert res["fact_records_inserted"] > 0

    # Summary Check
    summary = DataWarehouseEngine.get_warehouse_summary()
    assert summary["status"] == "HEALTHY"
    assert summary["fact_record_count"] > 0
    assert summary["total_net_cost_usd"] > 0
    assert "dim_date" in summary["dimension_counts"]
    assert summary["dimension_counts"]["dim_date"] > 0

def test_warehouse_unknown_member_key_zero():
    # Verify key 0 exists in dimensions
    import pandas as pd
    dim_cloud = pd.read_parquet("./data/delta/warehouse/dim_cloud/dim_cloud.parquet")
    assert 0 in dim_cloud['cloud_key'].values
    unknown_row = dim_cloud[dim_cloud['cloud_key'] == 0].iloc[0]
    assert unknown_row['cloud_provider'] == 'UNKNOWN'

def test_warehouse_idempotency():
    # Load again with same dataset_id & batch_id
    initial_summary = DataWarehouseEngine.get_warehouse_summary()
    initial_count = initial_summary["fact_record_count"]

    res_rerun = DataWarehouseEngine.load_warehouse_from_silver("ds_sample_test", "batch-001")
    assert res_rerun["status"] == "SUCCESS"

    post_summary = DataWarehouseEngine.get_warehouse_summary()
    # Count should remain identical because of idempotent record_hash deduplication
    assert post_summary["fact_record_count"] == initial_count

def test_analytical_queries_execution():
    queries = DataWarehouseEngine.execute_analytical_queries()
    assert len(queries) == 15
    q1 = next(q for q in queries if q["id"] == 1)
    assert q1["result"][0]["total_cost"] > 0

def test_scd2_simulation():
    scd_res = DataWarehouseEngine.simulate_scd2_update("prj-analytics", "staging")
    assert scd_res["status"] in ["SUCCESS", "NOT_FOUND"]
    if scd_res["status"] == "SUCCESS":
        assert scd_res["new_environment"] == "staging"
        assert scd_res["scd_type"] == "SCD Type 2"

def test_warehouse_fastapi_endpoints():
    r_summary = client.get("/api/warehouse/summary")
    assert r_summary.status_code == 200
    assert r_summary.json()["status"] == "HEALTHY"

    r_queries = client.get("/api/warehouse/queries")
    assert r_queries.status_code == 200
    assert len(r_queries.json()) == 15

    r_providers = client.get("/api/warehouse/providers")
    assert r_providers.status_code == 200

    r_schema = client.get("/api/warehouse/schema")
    assert r_schema.status_code == 200
    assert r_schema.json()["catalog"] == "cloud_cost_catalog"
    assert r_schema.json()["fact_table"]["name"] == "fact_cloud_cost"
