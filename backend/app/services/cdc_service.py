import os
import json
import logging
import hashlib
from datetime import datetime
from typing import Dict, Any, List, Optional
import pandas as pd
from sqlalchemy.orm import Session

from backend.app.core.config import settings
from backend.app.models.cdc import KafkaEventAuditModel, PipelineEventMetricsModel

logger = logging.getLogger("cdc_service")
logging.basicConfig(level=logging.INFO)

class CDCService:
    @classmethod
    def generate_business_key(cls, record: Dict[str, Any]) -> str:
        """
        Generates a deterministic business key for cloud cost records based on dimension attributes.
        
        Business Key Components:
        - cloud_provider
        - account_id
        - project_id
        - environment
        - region
        - service
        - resource_type
        - usage_unit
        - date
        
        Logic:
        Combines clean normalized dimension values separated by '|'.
        Guarantees that the exact same logical cloud usage item produces the same business key across sessions.
        """
        cloud_provider = str(record.get('cloud_provider', 'AWS')).strip().upper()
        account_id = str(record.get('account_id', 'ACC-000')).strip()
        project_id = str(record.get('project_id', 'PRJ-GENERIC')).strip()
        environment = str(record.get('environment', 'prod')).strip().lower()
        region = str(record.get('region', 'us-east-1')).strip().lower()
        service = str(record.get('service', 'GeneralService')).strip()
        resource_type = str(record.get('resource_type', 'Resource')).strip()
        usage_unit = str(record.get('usage_unit', 'Units')).strip().lower()
        date_str = str(record.get('date', '2026-08-01')).strip()

        raw_key = f"{cloud_provider}|{account_id}|{project_id}|{environment}|{region}|{service}|{resource_type}|{usage_unit}|{date_str}"
        return raw_key

    @classmethod
    def process_cdc_event(cls, event_data: Dict[str, Any], db: Session) -> Dict[str, Any]:
        """
        Executes complete CDC event processing (INSERT, UPDATE, DELETE) against Bronze CDC and Silver Delta Lake.
        Handles idempotency, out-of-order resolution, and feature engineering.
        """
        event_id = event_data.get('event_id')
        dataset_id = event_data.get('dataset_id', 'ds_streaming')
        batch_id = event_data.get('batch_id', 'batch_cdc')
        operation = str(event_data.get('operation', 'INSERT')).upper()
        raw_record = event_data.get('record', {})

        # Parse timestamp
        event_ts_str = event_data.get('event_timestamp')
        if event_ts_str:
            try:
                event_ts = datetime.fromisoformat(event_ts_str.replace('Z', '+00:00'))
            except Exception:
                event_ts = datetime.utcnow()
        else:
            event_ts = datetime.utcnow()

        # Generate Business Key & Record Hash
        business_key = cls.generate_business_key(raw_record)
        record_json_str = json.dumps(raw_record, sort_keys=True)
        record_hash = hashlib.sha256(record_json_str.encode('utf-8')).hexdigest()

        # Step 1: Append Event to Bronze CDC Lake Parquet
        cls._append_to_bronze_cdc(
            event_id=event_id,
            dataset_id=dataset_id,
            batch_id=batch_id,
            business_key=business_key,
            operation=operation,
            event_ts=event_ts,
            record_hash=record_hash,
            raw_record=raw_record
        )

        # Step 2: MERGE into Silver Delta Cleaned Parquet Layer
        silver_status, silver_msg = cls._merge_to_silver_cdc(
            event_id=event_id,
            dataset_id=dataset_id,
            business_key=business_key,
            operation=operation,
            event_ts=event_ts,
            raw_record=raw_record
        )

        return {
            "status": silver_status,
            "event_id": event_id,
            "business_key": business_key,
            "operation": operation,
            "message": silver_msg
        }

    @classmethod
    def _append_to_bronze_cdc(
        cls,
        event_id: str,
        dataset_id: str,
        batch_id: str,
        business_key: str,
        operation: str,
        event_ts: datetime,
        record_hash: str,
        raw_record: Dict[str, Any]
    ):
        """
        Persists raw CDC streaming events to Bronze CDC Delta Lake directory.
        """
        os.makedirs("./data/delta/bronze_cloud_cost_cdc", exist_ok=True)
        file_path = f"./data/delta/bronze_cloud_cost_cdc/{dataset_id}.parquet"

        cdc_row = {
            "event_id": event_id,
            "dataset_id": dataset_id,
            "batch_id": batch_id,
            "business_key": business_key,
            "operation": operation,
            "event_timestamp": event_ts.isoformat(),
            "record_hash": record_hash,
            "ingestion_timestamp": datetime.utcnow().isoformat(),
            "source": "KAFKA_CDC",
            "raw_record": json.dumps(raw_record),
            "processing_status": "PROCESSED"
        }

        new_df = pd.DataFrame([cdc_row])

        if os.path.exists(file_path):
            try:
                existing_df = pd.read_parquet(file_path)
                combined_df = pd.concat([existing_df, new_df], ignore_index=True)
                combined_df.to_parquet(file_path, index=False)
            except Exception as e:
                logger.error(f"[CDC] Error writing to Bronze CDC Parquet: {e}")
                new_df.to_parquet(file_path, index=False)
        else:
            new_df.to_parquet(file_path, index=False)

    @classmethod
    def _merge_to_silver_cdc(
        cls,
        event_id: str,
        dataset_id: str,
        business_key: str,
        operation: str,
        event_ts: datetime,
        raw_record: Dict[str, Any]
    ) -> (str, str):
        """
        Executes Delta MERGE against Silver layer based on CDC Operation (INSERT / UPDATE / DELETE).
        Includes Out-of-Order resolution and Soft-Delete strategy.
        """
        os.makedirs("./data/delta/silver_cloud_cost_clean", exist_ok=True)
        file_path = f"./data/delta/silver_cloud_cost_clean/{dataset_id}.parquet"

        if os.path.exists(file_path):
            try:
                df_silver = pd.read_parquet(file_path)
            except Exception as e:
                logger.error(f"[CDC] Error loading Silver Parquet: {e}")
                df_silver = pd.DataFrame()
        else:
            df_silver = pd.DataFrame()

        # Ensure CDC metadata columns exist in Silver dataframe
        cdc_columns = ['business_key', 'created_at', 'updated_at', 'is_deleted', 'deleted_at', 'last_event_id', 'last_operation']
        for col in cdc_columns:
            if col not in df_silver.columns:
                if col == 'is_deleted':
                    df_silver[col] = False
                else:
                    df_silver[col] = None

        existing_mask = (df_silver['business_key'] == business_key) if 'business_key' in df_silver.columns and not df_silver.empty else pd.Series([], dtype=bool)
        matching_indices = df_silver.index[existing_mask].tolist() if not existing_mask.empty else []

        event_ts_iso = event_ts.isoformat()

        if operation == "DELETE":
            # Soft Delete Strategy
            if matching_indices:
                idx = matching_indices[0]
                existing_updated_at = str(df_silver.at[idx, 'updated_at'] or '')
                # Out-of-Order Check
                if existing_updated_at and existing_updated_at > event_ts_iso:
                    return "OUT_OF_ORDER_SKIPPED", f"Older DELETE event ({event_ts_iso}) skipped for existing record updated at {existing_updated_at}."

                df_silver.at[idx, 'is_deleted'] = True
                df_silver.at[idx, 'deleted_at'] = event_ts_iso
                df_silver.at[idx, 'updated_at'] = event_ts_iso
                df_silver.at[idx, 'last_event_id'] = event_id
                df_silver.at[idx, 'last_operation'] = "DELETE"
                df_silver.to_parquet(file_path, index=False)
                return "SUCCESS", f"Soft-deleted business key {business_key} in Silver layer."
            else:
                # Insert placeholder soft-deleted record for lineage completeness
                record_data = cls._prepare_silver_record(raw_record, business_key)
                record_data.update({
                    "business_key": business_key,
                    "created_at": event_ts_iso,
                    "updated_at": event_ts_iso,
                    "is_deleted": True,
                    "deleted_at": event_ts_iso,
                    "last_event_id": event_id,
                    "last_operation": "DELETE"
                })
                df_new = pd.DataFrame([record_data])
                df_silver = pd.concat([df_silver, df_new], ignore_index=True) if not df_silver.empty else df_new
                df_silver.to_parquet(file_path, index=False)
                return "SUCCESS", f"Recorded soft delete for new business key {business_key}."

        elif operation in ["INSERT", "UPDATE"]:
            record_data = cls._prepare_silver_record(raw_record, business_key)

            if matching_indices:
                idx = matching_indices[0]
                existing_updated_at = str(df_silver.at[idx, 'updated_at'] or '')
                # Out-of-Order Check: if incoming event is older than latest recorded update, reject
                if existing_updated_at and existing_updated_at > event_ts_iso:
                    return "OUT_OF_ORDER_SKIPPED", f"Older {operation} event ({event_ts_iso}) skipped because existing record has newer timestamp ({existing_updated_at})."

                # Update existing record
                created_at_val = df_silver.at[idx, 'created_at'] or event_ts_iso
                for col, val in record_data.items():
                    df_silver.at[idx, col] = val

                df_silver.at[idx, 'business_key'] = business_key
                df_silver.at[idx, 'created_at'] = created_at_val
                df_silver.at[idx, 'updated_at'] = event_ts_iso
                df_silver.at[idx, 'is_deleted'] = False
                df_silver.at[idx, 'deleted_at'] = None
                df_silver.at[idx, 'last_event_id'] = event_id
                df_silver.at[idx, 'last_operation'] = operation
                df_silver.to_parquet(file_path, index=False)
                return "SUCCESS", f"Updated Silver record for business key {business_key} ({operation})."
            else:
                # Insert new record
                record_data.update({
                    "business_key": business_key,
                    "created_at": event_ts_iso,
                    "updated_at": event_ts_iso,
                    "is_deleted": False,
                    "deleted_at": None,
                    "last_event_id": event_id,
                    "last_operation": operation
                })
                df_new = pd.DataFrame([record_data])
                df_silver = pd.concat([df_silver, df_new], ignore_index=True) if not df_silver.empty else df_new
                df_silver.to_parquet(file_path, index=False)
                return "SUCCESS", f"Inserted new Silver record for business key {business_key} ({operation})."

        return "FAILED", f"Unsupported CDC operation: {operation}"

    @classmethod
    def _prepare_silver_record(cls, raw: Dict[str, Any], business_key: str) -> Dict[str, Any]:
        """
        Cleans raw record and applies Phase 2 Feature Engineering logic.
        """
        net_cost = float(raw.get('net_cost', 0.0) or 0.0)
        list_cost = float(raw.get('list_cost', net_cost) or net_cost)
        discount_amount = float(raw.get('discount_amount', 0.0) or 0.0)
        budget_amount = float(raw.get('budget_amount', 1000.0) or 1000.0)
        forecast_monthly = float(raw.get('forecast_monthly_cost', net_cost * 30.0) or (net_cost * 30.0))
        usage_qty = float(raw.get('usage_quantity', 1.0) or 1.0)

        # Calculated features
        total_savings = list_cost - net_cost if list_cost > net_cost else discount_amount
        effective_discount_pct = (total_savings / list_cost * 100.0) if list_cost > 0 else 0.0
        budget_remaining = budget_amount - net_cost
        forecast_variance = forecast_monthly - budget_amount
        cost_per_usage = (net_cost / usage_qty) if usage_qty > 0 else net_cost

        # Risk Classification
        if budget_amount > 0 and (net_cost / budget_amount) > 0.9:
            cost_risk = "HIGH"
            high_budget_flag = True
        elif budget_amount > 0 and (net_cost / budget_amount) > 0.7:
            cost_risk = "MEDIUM"
            high_budget_flag = False
        else:
            cost_risk = "LOW"
            high_budget_flag = False

        record = dict(raw)
        record.update({
            "net_cost": net_cost,
            "list_cost": list_cost,
            "total_savings": round(total_savings, 2),
            "effective_discount_pct": round(effective_discount_pct, 2),
            "budget_remaining": round(budget_remaining, 2),
            "forecast_variance": round(forecast_variance, 2),
            "cost_per_usage": round(cost_per_usage, 4),
            "cost_risk_level": cost_risk,
            "high_budget_utilization_flag": high_budget_flag
        })
        return record

    @classmethod
    def get_cdc_history(cls, business_key: str, dataset_id: str = "ds_sample_test") -> Dict[str, Any]:
        """
        Retrieves complete CDC event history and current state for a logical record.
        """
        # Read Bronze CDC history
        bronze_file = f"./data/delta/bronze_cloud_cost_cdc/{dataset_id}.parquet"
        events = []
        if os.path.exists(bronze_file):
            try:
                df_bronze = pd.read_parquet(bronze_file)
                if 'business_key' in df_bronze.columns:
                    matched = df_bronze[df_bronze['business_key'] == business_key]
                    events = matched.to_dict(orient="records")
            except Exception as e:
                logger.error(f"[CDC] Error reading Bronze CDC history: {e}")

        # Read Silver current state
        silver_file = f"./data/delta/silver_cloud_cost_clean/{dataset_id}.parquet"
        current_state = None
        is_deleted = False
        if os.path.exists(silver_file):
            try:
                df_silver = pd.read_parquet(silver_file)
                if 'business_key' in df_silver.columns:
                    matched = df_silver[df_silver['business_key'] == business_key]
                    if not matched.empty:
                        record_dict = matched.iloc[0].where(pd.notnull(matched.iloc[0]), None).to_dict()
                        is_deleted = bool(record_dict.get('is_deleted', False))
                        current_state = record_dict
            except Exception as e:
                logger.error(f"[CDC] Error reading Silver state: {e}")

        return {
            "business_key": business_key,
            "record_count": len(events),
            "is_deleted": is_deleted,
            "current_state": current_state,
            "history": events
        }
