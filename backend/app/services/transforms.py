from typing import Iterable, List, Dict, Any


def _clean_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _coerce_float(value: Any, default: float = 0.0) -> float:
    if value is None or value == "":
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def normalize_cloud_cost_rows(rows: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Normalize raw cloud-cost CSV rows into consistent pipeline-ready values."""
    cleaned: List[Dict[str, Any]] = []

    for row in rows:
        normalized = {
            "date": _clean_text(row.get("date")),
            "cloud_provider": _clean_text(row.get("cloud_provider")).upper() or "UNKNOWN",
            "account_id": _clean_text(row.get("account_id")),
            "project_id": _clean_text(row.get("project_id")),
            "service": _clean_text(row.get("service")),
            "resource_type": _clean_text(row.get("resource_type")),
            "usage_quantity": _coerce_float(row.get("usage_quantity")),
            "list_cost": _coerce_float(row.get("list_cost")),
            "net_cost": _coerce_float(row.get("net_cost")),
            "budget_amount": _coerce_float(row.get("budget_amount")),
        }

        if normalized["date"]:
            normalized["date"] = normalized["date"].strip()

        if normalized["cloud_provider"] == "":
            normalized["cloud_provider"] = "UNKNOWN"

        cleaned.append(normalized)

    return cleaned
