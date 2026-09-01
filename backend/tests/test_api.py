import os
from fastapi.testclient import TestClient
from backend.app.main import app

client = TestClient(app)


def test_read_root():
    response = client.get("/")
    assert response.status_code == 200
    json_data = response.json()
    assert "Phase 1" in json_data["phase"]

def test_health_check():
    response = client.get("/api/health")
    assert response.status_code == 200
    json_data = response.json()
    assert json_data["status"] == "healthy"
    assert "database" in json_data

def test_upload_valid_csv():
    csv_content = b"date,cloud_provider,net_cost\n2026-08-01,AWS,100.50\n2026-08-02,Azure,200.00\n"
    files = {"file": ("test_cloud_cost.csv", csv_content, "text/csv")}
    response = client.post("/api/datasets/upload", files=files)
    assert response.status_code == 201
    json_data = response.json()
    assert "id" in json_data
    assert json_data["row_count"] == 2
    assert json_data["column_count"] == 3
    assert json_data["status"] == "UPLOADED"
    
    dataset_id = json_data["id"]

    # Test preview
    preview_res = client.get(f"/api/datasets/{dataset_id}/preview")
    assert preview_res.status_code == 200
    preview_data = preview_res.json()
    assert len(preview_data["preview_data"]) == 2
    assert preview_data["columns"] == ["date", "cloud_provider", "net_cost"]

    # Test pipeline status
    status_res = client.get(f"/api/pipeline/status/{dataset_id}")
    assert status_res.status_code == 200
    status_data = status_res.json()
    assert status_data["status"] == "UPLOADED"

def test_upload_empty_csv():
    files = {"file": ("empty.csv", b"", "text/csv")}
    response = client.post("/api/datasets/upload", files=files)
    assert response.status_code == 400
    assert "empty" in response.json()["detail"].lower()

def test_upload_invalid_type():
    files = {"file": ("image.png", b"\x89PNG\r\n\x1a\n", "image/png")}
    response = client.post("/api/datasets/upload", files=files)
    assert response.status_code == 400
    assert "only csv" in response.json()["detail"].lower()
