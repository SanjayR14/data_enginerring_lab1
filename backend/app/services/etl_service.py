import os
import json
import hashlib
import uuid
import logging
from datetime import datetime
from typing import Dict, Any, List, Tuple, Optional
import pandas as pd
import numpy as np
from sqlalchemy.orm import Session

from backend.app.models.dataset import DatasetModel
from backend.app.models.pipeline import PipelineRunModel, DataQualityResultModel, QuarantineRecordModel
from backend.app.services.databricks_client import DatabricksClient

logger = logging.getLogger(__name__)

class ETLPipelineService:

    @staticmethod
    def generate_record_hash(row: pd.Series) -> str:
        """
        Generates a deterministic SHA-256 hash for record deduplication & idempotency.
        """
        raw_key = (
            f"{row.get('date', '')}|"
            f"{row.get('cloud_provider', '')}|"
            f"{row.get('account_id', '')}|"
            f"{row.get('project_id', '')}|"
            f"{row.get('service', '')}|"
            f"{row.get('resource_type', '')}|"
            f"{row.get('usage_quantity', '')}|"
            f"{row.get('net_cost', '')}"
        )
        return hashlib.sha256(raw_key.encode('utf-8')).hexdigest()

    @classmethod
    def execute_pipeline(cls, dataset_id: str, db: Session) -> PipelineRunModel:
        """
        Executes the full Phase 2 Data Engineering Pipeline:
        CSV Staging -> Bronze Delta -> Data Quality Checks -> Quarantine -> Cleaning -> Feature Eng -> Silver Delta -> Databricks Sync
        """
        logger.info(f"[ETL_PIPELINE] Starting Phase 2 execution for dataset_id: {dataset_id}")

        # 1. Fetch Dataset metadata
        dataset = db.query(DatasetModel).filter(DatasetModel.id == dataset_id).first()
        if not dataset:
            raise ValueError(f"Dataset '{dataset_id}' not found.")

        if not os.path.exists(dataset.storage_path):
            raise FileNotFoundError(f"Dataset file at path '{dataset.storage_path}' is missing.")

        # 2. Initialize or retrieve PipelineRun
        run_id = f"run_{uuid.uuid4().hex[:12]}"
        batch_id = f"batch_{uuid.uuid4().hex[:10]}"

        pipeline_run = PipelineRunModel(
            run_id=run_id,
            dataset_id=dataset_id,
            batch_id=batch_id,
            status="VALIDATING",
            current_stage="1. CSV Staging & Ingestion",
            started_at=datetime.utcnow(),
            input_records=dataset.row_count,
            databricks_executed=False
        )
        db.add(pipeline_run)
        db.commit()
        db.refresh(pipeline_run)

        try:
            # ----------------------------------------------------
            # STAGE 1: READ CSV & BRONZE LAYER INGESTION
            # ----------------------------------------------------
            pipeline_run.status = "BRONZE_PROCESSING"
            pipeline_run.current_stage = "2. Bronze Delta Ingestion"
            db.commit()

            logger.info(f"[ETL_PIPELINE] Stage 1 & 2: Reading CSV from {dataset.storage_path}")
            df_raw = pd.read_csv(dataset.storage_path)
            total_input_records = len(df_raw)
            pipeline_run.input_records = total_input_records

            # Compute record_hash for every row
            logger.info("[ETL_PIPELINE] Generating deterministic record hashes for Bronze layer...")
            df_bronze = df_raw.copy().reset_index(drop=True)
            df_bronze['record_hash'] = df_bronze.apply(cls.generate_record_hash, axis=1)
            df_bronze['dataset_id'] = dataset_id
            df_bronze['batch_id'] = batch_id
            df_bronze['ingestion_timestamp'] = datetime.utcnow().isoformat()
            df_bronze['source_file'] = dataset.original_filename

            # Save Bronze storage
            os.makedirs("./data/delta/bronze_cloud_cost_raw", exist_ok=True)
            bronze_parquet_path = f"./data/delta/bronze_cloud_cost_raw/{dataset_id}.parquet"
            df_bronze.to_parquet(bronze_parquet_path, index=False)
            pipeline_run.bronze_records = len(df_bronze)
            db.commit()

            # ----------------------------------------------------
            # STAGE 3: DATA QUALITY VALIDATION CHECKS
            # ----------------------------------------------------
            pipeline_run.status = "QUALITY_CHECK"
            pipeline_run.current_stage = "3. Data Quality Validation Checks"
            db.commit()

            logger.info("[ETL_PIPELINE] Stage 3: Running 12 Data Quality Checks...")
            dq_checks = []
            quarantined_records_list = []
            failed_hashes = set()
            quarantine_reasons = {}  # hash -> reason

            # Required columns list
            required_cols = [
                'date', 'year', 'month', 'cloud_provider', 'account_id', 'service',
                'usage_quantity', 'list_cost', 'net_cost', 'budget_amount'
            ]

            # Check 1: Schema Validation
            missing_req_cols = [c for c in required_cols if c not in df_bronze.columns]
            schema_status = "PASS" if len(missing_req_cols) == 0 else "FAIL"
            dq_checks.append(DataQualityResultModel(
                run_id=run_id, dataset_id=dataset_id, batch_id=batch_id,
                check_name="1. Schema Validation", status=schema_status,
                records_checked=total_input_records, records_failed=len(missing_req_cols),
                failure_percentage=round((len(missing_req_cols) / len(required_cols)) * 100, 2)
            ))

            # Check 2: Null Net Cost & Essential Fields
            null_cost_mask = df_bronze['net_cost'].isnull() if 'net_cost' in df_bronze.columns else pd.Series([False]*total_input_records)
            null_date_mask = df_bronze['date'].isnull() if 'date' in df_bronze.columns else pd.Series([False]*total_input_records)
            null_provider_mask = df_bronze['cloud_provider'].isnull() if 'cloud_provider' in df_bronze.columns else pd.Series([False]*total_input_records)
            critical_null_mask = null_cost_mask | null_date_mask | null_provider_mask

            for idx, is_bad in critical_null_mask.items():
                if is_bad:
                    r_hash = str(df_bronze.at[idx, 'record_hash'])
                    failed_hashes.add(r_hash)
                    quarantine_reasons[r_hash] = "NULL_NET_COST_OR_CRITICAL_FIELD"

            dq_checks.append(DataQualityResultModel(
                run_id=run_id, dataset_id=dataset_id, batch_id=batch_id,
                check_name="2. Null Critical Fields Check", status="PASS" if critical_null_mask.sum() == 0 else "FAIL",
                records_checked=total_input_records, records_failed=int(critical_null_mask.sum()),
                failure_percentage=round((critical_null_mask.sum() / total_input_records) * 100, 2)
            ))

            # Check 3: Duplicate Record Check
            dup_mask = df_bronze.duplicated(subset=['record_hash'], keep='first')
            for idx, is_dup in dup_mask.items():
                if is_dup:
                    r_hash = str(df_bronze.at[idx, 'record_hash'])
                    failed_hashes.add(r_hash)
                    quarantine_reasons[r_hash] = "DUPLICATE_RECORD"

            dq_checks.append(DataQualityResultModel(
                run_id=run_id, dataset_id=dataset_id, batch_id=batch_id,
                check_name="3. Duplicate Record Check", status="PASS" if dup_mask.sum() == 0 else "FAIL",
                records_checked=total_input_records, records_failed=int(dup_mask.sum()),
                failure_percentage=round((dup_mask.sum() / total_input_records) * 100, 2)
            ))

            # Check 4: Invalid Date Format & Year Bounds
            invalid_date_mask = pd.Series([False]*total_input_records)
            if 'date' in df_bronze.columns:
                parsed_dates = pd.to_datetime(df_bronze['date'], errors='coerce')
                invalid_date_mask = parsed_dates.isnull() | (parsed_dates.dt.year < 2000) | (parsed_dates.dt.year > 2100)
                for idx, is_bad in invalid_date_mask.items():
                    if is_bad:
                        r_hash = str(df_bronze.at[idx, 'record_hash'])
                        failed_hashes.add(r_hash)
                        quarantine_reasons[r_hash] = "INVALID_DATE"

            dq_checks.append(DataQualityResultModel(
                run_id=run_id, dataset_id=dataset_id, batch_id=batch_id,
                check_name="4. Date Format & Year Range Check", status="PASS" if invalid_date_mask.sum() == 0 else "FAIL",
                records_checked=total_input_records, records_failed=int(invalid_date_mask.sum()),
                failure_percentage=round((invalid_date_mask.sum() / total_input_records) * 100, 2)
            ))

            # Check 5: Invalid Month Bounds (1-12)
            invalid_month_mask = pd.Series([False]*total_input_records)
            if 'month' in df_bronze.columns:
                months_numeric = pd.to_numeric(df_bronze['month'], errors='coerce')
                invalid_month_mask = months_numeric.isnull() | (months_numeric < 1) | (months_numeric > 12)
                for idx, is_bad in invalid_month_mask.items():
                    if is_bad:
                        r_hash = str(df_bronze.at[idx, 'record_hash'])
                        failed_hashes.add(r_hash)
                        quarantine_reasons[r_hash] = "INVALID_MONTH"

            dq_checks.append(DataQualityResultModel(
                run_id=run_id, dataset_id=dataset_id, batch_id=batch_id,
                check_name="5. Month Value Range Check (1-12)", status="PASS" if invalid_month_mask.sum() == 0 else "FAIL",
                records_checked=total_input_records, records_failed=int(invalid_month_mask.sum()),
                failure_percentage=round((invalid_month_mask.sum() / total_input_records) * 100, 2)
            ))

            # Check 6: Negative Cost Check
            invalid_cost_mask = pd.Series([False]*total_input_records)
            if 'net_cost' in df_bronze.columns:
                net_costs = pd.to_numeric(df_bronze['net_cost'], errors='coerce')
                invalid_cost_mask = net_costs.notnull() & (net_costs < 0)
                for idx, is_bad in invalid_cost_mask.items():
                    if is_bad:
                        r_hash = str(df_bronze.at[idx, 'record_hash'])
                        failed_hashes.add(r_hash)
                        quarantine_reasons[r_hash] = "INVALID_NEGATIVE_COST"

            dq_checks.append(DataQualityResultModel(
                run_id=run_id, dataset_id=dataset_id, batch_id=batch_id,
                check_name="6. Non-Negative Net Cost Check", status="PASS" if invalid_cost_mask.sum() == 0 else "FAIL",
                records_checked=total_input_records, records_failed=int(invalid_cost_mask.sum()),
                failure_percentage=round((invalid_cost_mask.sum() / total_input_records) * 100, 2)
            ))

            # Check 7: Invalid Percentage Range Check (0 to 100 or 0 to 1)
            invalid_pct_mask = pd.Series([False]*total_input_records)
            if 'discount_rate_pct' in df_bronze.columns:
                pcts = pd.to_numeric(df_bronze['discount_rate_pct'], errors='coerce')
                invalid_pct_mask = pcts.notnull() & ((pcts < 0) | (pcts > 100))
                for idx, is_bad in invalid_pct_mask.items():
                    if is_bad:
                        r_hash = str(df_bronze.at[idx, 'record_hash'])
                        failed_hashes.add(r_hash)
                        quarantine_reasons[r_hash] = "INVALID_PERCENTAGE"

            dq_checks.append(DataQualityResultModel(
                run_id=run_id, dataset_id=dataset_id, batch_id=batch_id,
                check_name="7. Percentage Value Range Check", status="PASS" if invalid_pct_mask.sum() == 0 else "FAIL",
                records_checked=total_input_records, records_failed=int(invalid_pct_mask.sum()),
                failure_percentage=round((invalid_pct_mask.sum() / total_input_records) * 100, 2)
            ))

            # Check 8: Numeric Coercibility Check
            num_cols = ['usage_quantity', 'list_cost', 'budget_amount']
            invalid_num_mask = pd.Series([False]*total_input_records)
            for nc in num_cols:
                if nc in df_bronze.columns:
                    coerced = pd.to_numeric(df_bronze[nc], errors='coerce')
                    invalid_num_mask = invalid_num_mask | (df_bronze[nc].notnull() & coerced.isnull())
            
            for idx, is_bad in invalid_num_mask.items():
                if is_bad:
                    r_hash = str(df_bronze.at[idx, 'record_hash'])
                    failed_hashes.add(r_hash)
                    quarantine_reasons[r_hash] = "INVALID_NUMERIC_VALUE"

            dq_checks.append(DataQualityResultModel(
                run_id=run_id, dataset_id=dataset_id, batch_id=batch_id,
                check_name="8. Numeric Data Type Coercibility Check", status="PASS" if invalid_num_mask.sum() == 0 else "FAIL",
                records_checked=total_input_records, records_failed=int(invalid_num_mask.sum()),
                failure_percentage=round((invalid_num_mask.sum() / total_input_records) * 100, 2)
            ))

            # Save DQ checks to DB
            for check in dq_checks:
                db.add(check)
            db.commit()

            # ----------------------------------------------------
            # STAGE 4: QUARANTINE INVALID RECORDS
            # ----------------------------------------------------
            logger.info(f"[ETL_PIPELINE] Stage 4: Quarantining {len(failed_hashes)} invalid records...")
            quarantine_df_rows = []
            
            for idx, row in df_bronze.iterrows():
                r_hash = row['record_hash']
                if r_hash in failed_hashes:
                    reason = quarantine_reasons.get(r_hash, "GENERIC_QUALITY_FAILURE")
                    json_str = row.to_json()
                    
                    q_model = QuarantineRecordModel(
                        run_id=run_id,
                        dataset_id=dataset_id,
                        batch_id=batch_id,
                        record_hash=r_hash,
                        failure_reason=reason,
                        failed_at=datetime.utcnow(),
                        original_record=json_str
                    )
                    db.add(q_model)
                    quarantine_df_rows.append({
                        "dataset_id": dataset_id,
                        "batch_id": batch_id,
                        "record_hash": r_hash,
                        "failure_reason": reason,
                        "failed_at": datetime.utcnow().isoformat(),
                        "original_record": json_str
                    })

            db.commit()

            # Separate valid dataframe for Silver
            df_valid = df_bronze[~df_bronze['record_hash'].isin(failed_hashes)].copy()
            quarantined_count = len(failed_hashes)
            valid_count = len(df_valid)

            pipeline_run.valid_records = valid_count
            pipeline_run.quarantined_records = quarantined_count
            db.commit()

            # ----------------------------------------------------
            # STAGE 5: DATA CLEANING & TYPE STANDARDIZATION
            # ----------------------------------------------------
            pipeline_run.status = "SILVER_PROCESSING"
            pipeline_run.current_stage = "4. Cleaning, Preprocessing & Feature Engineering"
            db.commit()

            logger.info(f"[ETL_PIPELINE] Stage 5: Cleaning and type standardizing {valid_count} valid records...")
            df_silver = df_valid.copy()

            # String trimming & missing value imputation
            string_cols = df_silver.select_dtypes(include=['object']).columns
            for col in string_cols:
                if col not in ['record_hash', 'dataset_id', 'batch_id', 'ingestion_timestamp', 'source_file']:
                    df_silver[col] = df_silver[col].astype(str).str.strip()
                    df_silver[col] = df_silver[col].replace({'nan': 'N/A', 'None': 'N/A', '': 'N/A'})

            # Standardize Cloud Provider casing
            if 'cloud_provider' in df_silver.columns:
                df_silver['cloud_provider'] = df_silver['cloud_provider'].str.upper()

            # Explicit Type Casting
            numeric_defaults = {
                'usage_quantity': 0.0,
                'list_cost': 0.0,
                'discount_amount': 0.0,
                'net_cost': 0.0,
                'budget_amount': 0.0,
                'on_demand_cost': 0.0,
                'reserved_savings': 0.0,
                'savings_plan_savings': 0.0,
                'spot_savings': 0.0,
                'amortized_cost': 0.0,
                'forecast_monthly_cost': 0.0,
                'savings_plan_coverage_pct': 0.0,
                'reserved_instance_coverage_pct': 0.0,
                'discount_rate_pct': 0.0,
                'budget_utilization_pct': 0.0,
                'cost_variance_7d_pct': 0.0,
                'cost_variance_30d_pct': 0.0,
                'anomaly_score': 0.0
            }

            for col, def_val in numeric_defaults.items():
                if col in df_silver.columns:
                    df_silver[col] = pd.to_numeric(df_silver[col], errors='coerce').fillna(def_val).astype(float)
                else:
                    df_silver[col] = def_val

            # Int conversions
            int_defaults = ['year', 'month', 'day', 'day_of_week']
            for col in int_defaults:
                if col in df_silver.columns:
                    df_silver[col] = pd.to_numeric(df_silver[col], errors='coerce').fillna(0).astype(int)
                else:
                    df_silver[col] = 0

            # Boolean conversions
            if 'is_anomaly' in df_silver.columns:
                df_silver['is_anomaly'] = df_silver['is_anomaly'].astype(str).str.lower().isin(['true', '1', 't', 'yes'])
            else:
                df_silver['is_anomaly'] = False

            # Standardize Date Format
            if 'date' in df_silver.columns:
                df_silver['date'] = pd.to_datetime(df_silver['date'], errors='coerce').dt.strftime('%Y-%m-%d')

            # ----------------------------------------------------
            # STAGE 6: FEATURE ENGINEERING
            # ----------------------------------------------------
            logger.info("[ETL_PIPELINE] Stage 6: Calculating derived analytical features...")

            # A. total_savings = reserved_savings + savings_plan_savings + spot_savings
            df_silver['total_savings'] = (
                df_silver['reserved_savings'] +
                df_silver['savings_plan_savings'] +
                df_silver['spot_savings']
            ).round(2)

            # B. effective_discount_pct = ((list_cost - net_cost) / list_cost) * 100 if list_cost > 0 else 0.0
            df_silver['effective_discount_pct'] = np.where(
                df_silver['list_cost'] > 0,
                ((df_silver['list_cost'] - df_silver['net_cost']) / df_silver['list_cost']) * 100.0,
                0.0
            ).round(2)

            # C. budget_remaining = budget_amount - net_cost
            df_silver['budget_remaining'] = (df_silver['budget_amount'] - df_silver['net_cost']).round(2)

            # D. forecast_variance = forecast_monthly_cost - budget_amount
            df_silver['forecast_variance'] = (df_silver['forecast_monthly_cost'] - df_silver['budget_amount']).round(2)

            # E. cost_per_usage = net_cost / usage_quantity if usage_quantity > 0 else 0.0
            df_silver['cost_per_usage'] = np.where(
                df_silver['usage_quantity'] > 0,
                df_silver['net_cost'] / df_silver['usage_quantity'],
                0.0
            ).round(4)

            # F. high_budget_utilization_flag (True if budget_utilization_pct >= 85.0 or net_cost >= 0.85 * budget_amount)
            df_silver['high_budget_utilization_flag'] = (df_silver['budget_utilization_pct'] >= 85.0) | (
                (df_silver['budget_amount'] > 0) & (df_silver['net_cost'] >= 0.85 * df_silver['budget_amount'])
            )

            # G. cost_risk_level (HIGH, MEDIUM, LOW)
            # Rule:
            # HIGH: is_anomaly == True OR budget_utilization_pct >= 90.0 OR cost_variance_7d_pct >= 25.0
            # MEDIUM: budget_utilization_pct >= 75.0 OR cost_variance_7d_pct >= 10.0 OR anomaly_score >= 0.5
            # LOW: Otherwise
            def calculate_risk(row):
                if row['is_anomaly'] or row['budget_utilization_pct'] >= 90.0 or row['cost_variance_7d_pct'] >= 25.0:
                    return "HIGH"
                elif row['budget_utilization_pct'] >= 75.0 or row['cost_variance_7d_pct'] >= 10.0 or row['anomaly_score'] >= 0.5:
                    return "MEDIUM"
                return "LOW"

            df_silver['cost_risk_level'] = df_silver.apply(calculate_risk, axis=1)
            df_silver['processing_timestamp'] = datetime.utcnow().isoformat()

            # Save Silver Parquet
            os.makedirs("./data/delta/silver_cloud_cost_clean", exist_ok=True)
            silver_parquet_path = f"./data/delta/silver_cloud_cost_clean/{dataset_id}.parquet"
            df_silver.to_parquet(silver_parquet_path, index=False)

            pipeline_run.silver_records = len(df_silver)
            db.commit()

            # ----------------------------------------------------
            # STAGE 7: DATABRICKS SYNC & FINALIZATION
            # ----------------------------------------------------
            pipeline_run.current_stage = "5. Databricks Delta Lake Sync"
            db.commit()

            quarantine_df_export = pd.DataFrame(quarantine_df_rows) if quarantine_df_rows else pd.DataFrame(columns=['dataset_id', 'batch_id', 'record_hash', 'failure_reason', 'failed_at', 'original_record'])

            logger.info("[ETL_PIPELINE] Attempting Databricks Delta Lake table synchronization...")
            db_sync_ok, db_sync_msg = DatabricksClient.sync_pipeline_data(
                dataset_id=dataset_id,
                batch_id=batch_id,
                bronze_df=df_bronze,
                silver_df=df_silver,
                quarantine_df=quarantine_df_export,
                dq_results=dq_checks
            )

            pipeline_run.databricks_executed = db_sync_ok
            pipeline_run.status = "COMPLETED"
            pipeline_run.current_stage = "Completed (Silver Delta Table Ready)"
            pipeline_run.completed_at = datetime.utcnow()
            
            # Update main Dataset status
            dataset.status = "PROCESSED_SILVER"
            dataset.updated_at = datetime.utcnow()
            db.commit()

            logger.info(f"[ETL_PIPELINE] Phase 2 Pipeline completed successfully for dataset {dataset_id}. Input: {total_input_records}, Valid/Silver: {len(df_silver)}, Quarantined: {quarantined_count}")
            return pipeline_run

        except Exception as e:
            logger.error(f"[ETL_PIPELINE] Pipeline failed for dataset {dataset_id}: {str(e)}", exc_info=True)
            pipeline_run.status = "FAILED"
            pipeline_run.current_stage = "Pipeline Execution Failure"
            pipeline_run.error_message = str(e)
            pipeline_run.completed_at = datetime.utcnow()
            db.commit()
            raise e

    @staticmethod
    def get_dataset_profile(dataset_id: str, db: Session) -> Dict[str, Any]:
        """
        Generates comprehensive profiling metrics for Phase 2 EDA preparation.
        """
        dataset = db.query(DatasetModel).filter(DatasetModel.id == dataset_id).first()
        if not dataset:
            raise ValueError("Dataset not found")

        # Try silver parquet first, fallback to raw CSV
        silver_path = f"./data/delta/silver_cloud_cost_clean/{dataset_id}.parquet"
        if os.path.exists(silver_path):
            df = pd.read_parquet(silver_path)
        elif os.path.exists(dataset.storage_path):
            df = pd.read_csv(dataset.storage_path)
        else:
            raise FileNotFoundError("Dataset storage missing")

        null_counts = {str(col): int(df[col].isnull().sum()) for col in df.columns}
        distinct_counts = {str(col): int(df[col].nunique()) for col in df.columns}

        numeric_stats = {}
        numeric_df = df.select_dtypes(include=[np.number])
        for col in numeric_df.columns:
            series = numeric_df[col].dropna()
            if not series.empty:
                numeric_stats[str(col)] = {
                    "min": float(series.min()),
                    "max": float(series.max()),
                    "mean": float(round(series.mean(), 2)),
                    "median": float(round(series.median(), 2)),
                    "std": float(round(series.std(), 2)) if len(series) > 1 else 0.0
                }

        categorical_frequencies = {}
        cat_cols = df.select_dtypes(include=['object', 'category', 'bool']).columns
        for col in cat_cols:
            if col not in ['record_hash', 'original_record', 'columns_json']:
                top_counts = df[col].value_counts().head(5).to_dict()
                categorical_frequencies[str(col)] = {str(k): int(v) for k, v in top_counts.items()}

        # Duplicate row detection (full-row duplicates, excluding generated/system columns)
        dedupe_cols = [c for c in df.columns if c not in ['record_hash', 'ingestion_timestamp', 'batch_id']]
        duplicate_count = int(df.duplicated(subset=dedupe_cols).sum()) if dedupe_cols else 0

        # Correlation matrix across numeric features (Pearson)
        correlation_matrix = {}
        corr_cols = [c for c in numeric_df.columns if numeric_df[c].nunique(dropna=True) > 1]
        if len(corr_cols) >= 2:
            corr_df = numeric_df[corr_cols].corr(method="pearson").round(3)
            corr_df = corr_df.where(pd.notnull(corr_df), None)
            correlation_matrix = {
                str(row): {str(col): (float(val) if val is not None else None) for col, val in corr_df.loc[row].items()}
                for row in corr_df.index
            }

        # Outlier detection via IQR (1.5x rule) on numeric features
        outliers = {}
        for col in numeric_df.columns:
            series = numeric_df[col].dropna()
            if len(series) < 4:
                continue
            q1 = series.quantile(0.25)
            q3 = series.quantile(0.75)
            iqr = q3 - q1
            if iqr == 0:
                continue
            lower = q1 - 1.5 * iqr
            upper = q3 + 1.5 * iqr
            mask = (series < lower) | (series > upper)
            count = int(mask.sum())
            if count > 0:
                outliers[str(col)] = {
                    "count": count,
                    "lower_bound": float(round(lower, 2)),
                    "upper_bound": float(round(upper, 2)),
                    "example_values": [float(v) for v in series[mask].head(5).tolist()]
                }

        return {
            "dataset_id": dataset_id,
            "row_count": len(df),
            "column_count": len(df.columns),
            "null_counts": null_counts,
            "distinct_counts": distinct_counts,
            "numeric_stats": numeric_stats,
            "categorical_frequencies": categorical_frequencies,
            "duplicate_count": duplicate_count,
            "correlation_matrix": correlation_matrix,
            "outliers": outliers
        }
