import pandas as pd

from backend.app.services.transforms import normalize_cloud_cost_rows


def test_normalize_cloud_cost_rows_handles_missing_and_string_values():
    raw_rows = [
        {
            "date": " 2024-01-05 ",
            "cloud_provider": " aws ",
            "account_id": " acct-100 ",
            "project_id": " proj-1 ",
            "service": " ec2 ",
            "resource_type": " compute ",
            "usage_quantity": "12.5",
            "list_cost": "100",
            "net_cost": "82.00",
            "budget_amount": "90",
        },
        {
            "date": "2024-02-10",
            "cloud_provider": "GCP",
            "account_id": "acct-101",
            "project_id": "proj-2",
            "service": "bigquery",
            "resource_type": "analytics",
            "usage_quantity": "",
            "list_cost": "0",
            "net_cost": None,
            "budget_amount": "25",
        },
    ]

    cleaned = normalize_cloud_cost_rows(raw_rows)

    assert len(cleaned) == 2
    assert cleaned[0]["date"] == "2024-01-05"
    assert cleaned[0]["cloud_provider"] == "AWS"
    assert cleaned[0]["usage_quantity"] == 12.5
    assert cleaned[0]["list_cost"] == 100.0
    assert cleaned[0]["net_cost"] == 82.0

    assert cleaned[1]["usage_quantity"] == 0.0
    assert cleaned[1]["net_cost"] == 0.0
    assert cleaned[1]["cloud_provider"] == "GCP"


def test_normalize_cloud_cost_rows_returns_dataframe_for_pipeline_use():
    raw_rows = [
        {
            "date": "2024-03-01",
            "cloud_provider": "azure",
            "account_id": "acct-1",
            "project_id": "proj-a",
            "service": "sql",
            "resource_type": "db",
            "usage_quantity": "4",
            "list_cost": "50",
            "net_cost": "40",
            "budget_amount": "45",
        }
    ]

    df = pd.DataFrame(normalize_cloud_cost_rows(raw_rows))

    assert list(df.columns) == [
        "date",
        "cloud_provider",
        "account_id",
        "project_id",
        "service",
        "resource_type",
        "usage_quantity",
        "list_cost",
        "net_cost",
        "budget_amount",
    ]
    assert df["usage_quantity"].dtype.kind in {"f", "i"}
