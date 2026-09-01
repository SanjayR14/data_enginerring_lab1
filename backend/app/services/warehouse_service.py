"""
Cloud Cost Intelligence & Data Engineering Platform
Phase 5: Data Warehouse Engine, Star & Snowflake Schema Models, and Analytical Query Engine
Namespace: cloud_cost_catalog.cloud_warehouse
"""

import os
import json
import logging
import hashlib
from datetime import datetime, date
from typing import Dict, Any, List, Optional
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)

WAREHOUSE_DIR = "./data/delta/warehouse"

class DataWarehouseEngine:

    @staticmethod
    def _safe_read_parquet(path: str) -> pd.DataFrame:
        if not os.path.exists(path):
            return pd.DataFrame()
        try:
            return pd.read_parquet(path)
        except Exception:
            try:
                os.remove(path)
            except OSError:
                pass
            return pd.DataFrame()

    @staticmethod
    def _json_safe(value):
        if isinstance(value, dict):
            return {str(k): DataWarehouseEngine._json_safe(v) for k, v in value.items()}
        if isinstance(value, list):
            return [DataWarehouseEngine._json_safe(v) for v in value]
        if isinstance(value, tuple):
            return [DataWarehouseEngine._json_safe(v) for v in value]
        if isinstance(value, (np.floating, float)):
            if pd.isna(value) or not np.isfinite(value):
                return 0.0
            return float(value)
        if isinstance(value, (np.integer, int)):
            return int(value)
        if isinstance(value, (np.bool_, bool)):
            return bool(value)
        if value is None or (isinstance(value, str) and value.lower() in {"nan", "none", "null"}):
            return None
        return value

    @classmethod
    def _ensure_warehouse_dirs(cls):
        os.makedirs(WAREHOUSE_DIR, exist_ok=True)
        for dim in [
            "dim_date", "dim_cloud", "dim_account", "dim_project",
            "dim_organization", "dim_business_unit", "dim_department", "dim_cost_center",
            "dim_location", "dim_service", "dim_environment", "dim_currency",
            "fact_cloud_cost"
        ]:
            os.makedirs(os.path.join(WAREHOUSE_DIR, dim), exist_ok=True)

    @classmethod
    def load_warehouse_from_silver(cls, dataset_id: str, batch_id: str) -> Dict[str, Any]:
        """
        Idempotent Warehouse ETL Process:
        Silver Layer -> Surrogate Key Resolution -> Dimensions MERGE -> Fact MERGE -> Analytical Views
        """
        cls._ensure_warehouse_dirs()

        silver_path = f"./data/delta/silver_cloud_cost_clean/{dataset_id}.parquet"
        if not os.path.exists(silver_path):
            # Fallback to Bronze if Silver is missing
            silver_path = f"./data/delta/bronze_cloud_cost_raw/{dataset_id}.parquet"
            if not os.path.exists(silver_path):
                silver_path = "./data/sample/cloud_cost_dataset.csv"

        if silver_path.endswith(".csv"):
            df_silver = pd.read_csv(silver_path)
        else:
            df_silver = pd.read_parquet(silver_path)

        if df_silver.empty:
            raise ValueError("Silver layer is empty. Cannot populate Data Warehouse.")

        # Ensure required numerical and string columns exist
        for num_col in ['net_cost', 'list_cost', 'usage_quantity', 'budget_amount', 'reserved_savings', 'savings_plan_savings', 'spot_savings']:
            if num_col not in df_silver.columns:
                df_silver[num_col] = 0.0
            else:
                df_silver[num_col] = pd.to_numeric(df_silver[num_col], errors='coerce').fillna(0.0)

        for str_col in ['cloud_provider', 'account_id', 'project_id', 'business_unit', 'department', 'cost_center', 'region', 'service', 'resource_type', 'environment', 'currency', 'date']:
            if str_col not in df_silver.columns:
                if str_col == 'currency':
                    df_silver[str_col] = 'USD'
                elif str_col == 'environment':
                    df_silver[str_col] = 'production'
                elif str_col == 'date':
                    df_silver[str_col] = datetime.utcnow().strftime('%Y-%m-%d')
                else:
                    df_silver[str_col] = 'N/A'
            else:
                df_silver[str_col] = df_silver[str_col].astype(str).str.strip()

        # 1. LOAD DIMENSIONS (with Surrogate Keys & Unknown Member Key = 0)
        dim_date = cls._populate_dim_date(df_silver['date'].unique())
        dim_cloud = cls._populate_dim_cloud(df_silver['cloud_provider'].unique())
        dim_account = cls._populate_dim_account(df_silver[['account_id', 'cloud_provider']].drop_duplicates())
        dim_project = cls._populate_dim_project(df_silver[['project_id', 'account_id', 'environment']].drop_duplicates())
        dim_org = cls._populate_dim_organization(df_silver[['business_unit', 'department', 'cost_center']].drop_duplicates())
        dim_bu, dim_dept, dim_cc = cls._populate_snowflake_org_hierarchy(df_silver[['business_unit', 'department', 'cost_center']].drop_duplicates())
        dim_location = cls._populate_dim_location(df_silver['region'].unique())
        dim_service = cls._populate_dim_service(df_silver[['service', 'resource_type']].drop_duplicates())
        dim_env = cls._populate_dim_environment(df_silver['environment'].unique())
        dim_currency = cls._populate_dim_currency(df_silver['currency'].unique())

        # 2. SURROGATE KEY RESOLUTION FOR FACT TABLE
        df_fact = df_silver.copy()

        # Convert date to integer YYYYMMDD key
        def parse_date_key(d_str):
            try:
                dt = pd.to_datetime(d_str)
                return int(dt.strftime('%Y%m%d'))
            except Exception:
                return 0

        df_fact['date_key'] = df_fact['date'].apply(parse_date_key)

        # Map dim_cloud
        cloud_map = dict(zip(dim_cloud['cloud_provider'], dim_cloud['cloud_key']))
        df_fact['cloud_key'] = df_fact['cloud_provider'].str.upper().str.strip().map(cloud_map).fillna(0).astype(int)

        # Map dim_account
        account_map = dict(zip(dim_account['account_id'], dim_account['account_key']))
        df_fact['account_key'] = df_fact['account_id'].map(account_map).fillna(0).astype(int)

        # Map dim_project
        project_map = dict(zip(dim_project['project_id'], dim_project['project_key']))
        df_fact['project_key'] = df_fact['project_id'].map(project_map).fillna(0).astype(int)

        # Map dim_organization
        org_tuples = zip(dim_org['business_unit'], dim_org['department'], dim_org['cost_center'])
        org_map = {tup: key for tup, key in zip(org_tuples, dim_org['organization_key'])}
        df_fact['organization_key'] = df_fact.apply(
            lambda r: org_map.get((r['business_unit'], r['department'], r['cost_center']), 0), axis=1
        ).astype(int)

        # Map dim_location
        loc_map = dict(zip(dim_location['region'], dim_location['location_key']))
        df_fact['location_key'] = df_fact['region'].map(loc_map).fillna(0).astype(int)

        # Map dim_service
        service_tuples = zip(dim_service['service'], dim_service['resource_type'])
        service_map = {tup: key for tup, key in zip(service_tuples, dim_service['service_key'])}
        df_fact['service_key'] = df_fact.apply(
            lambda r: service_map.get((r['service'], r['resource_type']), 0), axis=1
        ).astype(int)

        # Map dim_environment
        env_map = dict(zip(dim_env['environment'], dim_env['environment_key']))
        df_fact['environment_key'] = df_fact['environment'].map(env_map).fillna(0).astype(int)

        # Map dim_currency
        curr_map = dict(zip(dim_currency['currency'], dim_currency['currency_key']))
        df_fact['currency_key'] = df_fact['currency'].map(curr_map).fillna(0).astype(int)

        # Ensure measures are computed
        df_fact['total_savings'] = (df_fact['reserved_savings'] + df_fact['savings_plan_savings'] + df_fact['spot_savings']).round(2)
        df_fact['discount_amount'] = (df_fact['list_cost'] - df_fact['net_cost']).clip(lower=0.0).round(2)
        df_fact['effective_discount_pct'] = np.where(df_fact['list_cost'] > 0, (df_fact['discount_amount'] / df_fact['list_cost']) * 100.0, 0.0).round(2)
        df_fact['budget_remaining'] = (df_fact['budget_amount'] - df_fact['net_cost']).round(2)
        # budget_utilization_pct is already provided per-row by the source data (net_cost is a
        # single line item, not the account's total monthly spend, so it can't be correctly
        # re-derived here). Only compute a fallback if it's genuinely missing.
        if 'budget_utilization_pct' not in df_fact.columns:
            df_fact['budget_utilization_pct'] = 0.0
        else:
            df_fact['budget_utilization_pct'] = pd.to_numeric(df_fact['budget_utilization_pct'], errors='coerce').fillna(0.0)

        for col in ['cost_variance_7d_pct', 'anomaly_score']:
            if col not in df_fact.columns:
                df_fact[col] = 0.0
            else:
                df_fact[col] = pd.to_numeric(df_fact[col], errors='coerce').fillna(0.0)
        if 'is_anomaly' not in df_fact.columns:
            df_fact['is_anomaly'] = False
        else:
            df_fact['is_anomaly'] = df_fact['is_anomaly'].astype(bool)

        def calc_risk(r):
            if r['is_anomaly'] or r['budget_utilization_pct'] >= 90.0 or r['cost_variance_7d_pct'] >= 25.0:
                return 'HIGH'
            elif r['budget_utilization_pct'] >= 75.0 or r['cost_variance_7d_pct'] >= 10.0 or r['anomaly_score'] >= 0.5:
                return 'MEDIUM'
            return 'LOW'

        df_fact['cost_risk_level'] = df_fact.apply(calc_risk, axis=1)
        df_fact['dataset_id'] = dataset_id
        df_fact['batch_id'] = batch_id

        # Unique fact identifier
        df_fact['record_hash'] = df_fact.apply(
            lambda r: hashlib.sha256(f"{r['date_key']}|{r['account_key']}|{r['project_key']}|{r['service_key']}|{r['net_cost']}".encode('utf-8')).hexdigest()[:16],
            axis=1
        )

        # Select fact columns
        fact_cols = [
            'record_hash', 'dataset_id', 'batch_id', 'date_key', 'cloud_key', 'account_key',
            'project_key', 'organization_key', 'location_key', 'service_key', 'environment_key',
            'currency_key', 'usage_quantity', 'list_cost', 'discount_amount', 'net_cost',
            'reserved_savings', 'savings_plan_savings', 'spot_savings', 'total_savings',
            'budget_amount', 'budget_remaining', 'budget_utilization_pct', 'effective_discount_pct',
            'cost_risk_level', 'is_anomaly'
        ]
        df_fact_clean = df_fact[fact_cols].copy()

        # 3. IDEMPOTENT FACT MERGE (Avoid duplicate records on rerun)
        fact_store_path = os.path.join(WAREHOUSE_DIR, "fact_cloud_cost", "fact_cloud_cost.parquet")
        if os.path.exists(fact_store_path):
            df_existing_fact = cls._safe_read_parquet(fact_store_path)
            if df_existing_fact.empty:
                df_final_fact = df_fact_clean
            else:
                existing_hashes = set(df_existing_fact['record_hash']) if 'record_hash' in df_existing_fact.columns else set()
                df_new_fact = df_fact_clean[~df_fact_clean['record_hash'].isin(existing_hashes)]
                df_final_fact = pd.concat([df_existing_fact, df_new_fact], ignore_index=True)
        else:
            df_final_fact = df_fact_clean

        df_final_fact.to_parquet(fact_store_path, index=False)

        # 4. REFRESH ANALYTICAL VIEWS
        cls.refresh_analytical_views()

        logger.info(f"[WAREHOUSE] Successfully loaded Data Warehouse. Fact rows: {len(df_final_fact)}")
        return {
            "status": "SUCCESS",
            "catalog": "cloud_cost_catalog",
            "schema": "cloud_warehouse",
            "dataset_id": dataset_id,
            "batch_id": batch_id,
            "fact_records_inserted": len(df_fact_clean),
            "fact_records_total": len(df_final_fact),
            "dimensions_loaded": 10
        }

    # -------------------------------------------------------------------------
    # DIMENSION POPULATION WITH SURROGATE KEYS & UNKNOWN MEMBER (Key = 0)
    # -------------------------------------------------------------------------

    @classmethod
    def _populate_dim_date(cls, dates_list) -> pd.DataFrame:
        path = os.path.join(WAREHOUSE_DIR, "dim_date", "dim_date.parquet")
        records = [{
            "date_key": 0, "date": "1970-01-01", "year": 1970, "quarter": 1,
            "month": 1, "month_name": "UNKNOWN", "week": 1, "day": 1,
            "day_of_week": 1, "day_name": "UNKNOWN", "is_month_start": False, "is_month_end": False
        }]

        for d_str in dates_list:
            try:
                dt = pd.to_datetime(d_str)
                d_key = int(dt.strftime('%Y%m%d'))
                records.append({
                    "date_key": d_key,
                    "date": dt.strftime('%Y-%m-%d'),
                    "year": dt.year,
                    "quarter": dt.quarter,
                    "month": dt.month,
                    "month_name": dt.strftime('%B'),
                    "week": int(dt.isocalendar().week),
                    "day": dt.day,
                    "day_of_week": dt.dayofweek + 1,
                    "day_name": dt.strftime('%A'),
                    "is_month_start": dt.is_month_start,
                    "is_month_end": dt.is_month_end
                })
            except Exception:
                pass

        df = pd.DataFrame(records).drop_duplicates(subset=['date_key'])
        df.to_parquet(path, index=False)
        return df

    @classmethod
    def _populate_dim_cloud(cls, providers_list) -> pd.DataFrame:
        path = os.path.join(WAREHOUSE_DIR, "dim_cloud", "dim_cloud.parquet")
        records = [{"cloud_key": 0, "cloud_provider": "UNKNOWN", "provider_group": "UNKNOWN"}]

        known_providers = {"AWS": "Amazon Web Services", "GCP": "Google Cloud Platform", "AZURE": "Microsoft Azure"}
        curr_key = 1
        for p in providers_list:
            p_str = str(p).upper().strip()
            if p_str and p_str != "N/A" and p_str != "UNKNOWN":
                records.append({
                    "cloud_key": curr_key,
                    "cloud_provider": p_str,
                    "provider_group": known_providers.get(p_str, "Other Cloud Provider")
                })
                curr_key += 1

        df = pd.DataFrame(records).drop_duplicates(subset=['cloud_provider'])
        df.to_parquet(path, index=False)
        return df

    @classmethod
    def _populate_dim_account(cls, account_df) -> pd.DataFrame:
        path = os.path.join(WAREHOUSE_DIR, "dim_account", "dim_account.parquet")
        records = [{
            "account_key": 0, "account_id": "UNKNOWN", "cloud_key": 0,
            "account_name": "UNKNOWN", "effective_date": "1970-01-01",
            "expiry_date": None, "is_current": True
        }]

        curr_key = 1
        for _, r in account_df.iterrows():
            acc_id = str(r['account_id']).strip()
            if acc_id and acc_id != "N/A" and acc_id != "UNKNOWN":
                records.append({
                    "account_key": curr_key,
                    "account_id": acc_id,
                    "cloud_key": 1,
                    "account_name": f"Account {acc_id}",
                    "effective_date": "2026-01-01",
                    "expiry_date": None,
                    "is_current": True
                })
                curr_key += 1

        df = pd.DataFrame(records).drop_duplicates(subset=['account_id'])
        df.to_parquet(path, index=False)
        return df

    @classmethod
    def _populate_dim_project(cls, project_df) -> pd.DataFrame:
        path = os.path.join(WAREHOUSE_DIR, "dim_project", "dim_project.parquet")
        records = [{
            "project_key": 0, "project_id": "UNKNOWN", "account_key": 0,
            "environment": "UNKNOWN", "effective_date": "1970-01-01",
            "expiry_date": None, "is_current": True
        }]

        curr_key = 1
        for _, r in project_df.iterrows():
            p_id = str(r['project_id']).strip()
            if p_id and p_id != "N/A" and p_id != "UNKNOWN":
                records.append({
                    "project_key": curr_key,
                    "project_id": p_id,
                    "account_key": 1,
                    "environment": str(r.get('environment', 'production')),
                    "effective_date": "2026-01-01",
                    "expiry_date": None,
                    "is_current": True
                })
                curr_key += 1

        df = pd.DataFrame(records).drop_duplicates(subset=['project_id'])
        df.to_parquet(path, index=False)
        return df

    @classmethod
    def _populate_dim_organization(cls, org_df) -> pd.DataFrame:
        path = os.path.join(WAREHOUSE_DIR, "dim_organization", "dim_organization.parquet")
        records = [{
            "organization_key": 0, "business_unit": "UNKNOWN",
            "department": "UNKNOWN", "cost_center": "UNKNOWN"
        }]

        curr_key = 1
        for _, r in org_df.iterrows():
            bu = str(r['business_unit']).strip()
            dept = str(r['department']).strip()
            cc = str(r['cost_center']).strip()
            records.append({
                "organization_key": curr_key,
                "business_unit": bu,
                "department": dept,
                "cost_center": cc
            })
            curr_key += 1

        df = pd.DataFrame(records).drop_duplicates(subset=['business_unit', 'department', 'cost_center'])
        df.to_parquet(path, index=False)
        return df

    @classmethod
    def _populate_snowflake_org_hierarchy(cls, org_df) -> tuple:
        bu_path = os.path.join(WAREHOUSE_DIR, "dim_business_unit", "dim_business_unit.parquet")
        dept_path = os.path.join(WAREHOUSE_DIR, "dim_department", "dim_department.parquet")
        cc_path = os.path.join(WAREHOUSE_DIR, "dim_cost_center", "dim_cost_center.parquet")

        bu_recs = [{"business_unit_key": 0, "business_unit": "UNKNOWN"}]
        dept_recs = [{"department_key": 0, "business_unit_key": 0, "department": "UNKNOWN"}]
        cc_recs = [{"cost_center_key": 0, "department_key": 0, "cost_center": "UNKNOWN"}]

        unique_bus = list(set(org_df['business_unit'].astype(str)))
        bu_map = {bu: idx + 1 for idx, bu in enumerate(unique_bus)}
        for bu, k in bu_map.items():
            bu_recs.append({"business_unit_key": k, "business_unit": bu})

        unique_depts = org_df[['department', 'business_unit']].drop_duplicates()
        dept_map = {}
        for idx, (_, r) in enumerate(unique_depts.iterrows()):
            dept_k = idx + 1
            bu_k = bu_map.get(str(r['business_unit']), 0)
            dept_recs.append({"department_key": dept_k, "business_unit_key": bu_k, "department": str(r['department'])})
            dept_map[str(r['department'])] = dept_k

        for idx, (_, r) in enumerate(org_df.iterrows()):
            cc_k = idx + 1
            dept_k = dept_map.get(str(r['department']), 0)
            cc_recs.append({"cost_center_key": cc_k, "department_key": dept_k, "cost_center": str(r['cost_center'])})

        df_bu = pd.DataFrame(bu_recs)
        df_dept = pd.DataFrame(dept_recs)
        df_cc = pd.DataFrame(cc_recs)

        df_bu.to_parquet(bu_path, index=False)
        df_dept.to_parquet(dept_path, index=False)
        df_cc.to_parquet(cc_path, index=False)

        return df_bu, df_dept, df_cc

    @classmethod
    def _populate_dim_location(cls, region_list) -> pd.DataFrame:
        path = os.path.join(WAREHOUSE_DIR, "dim_location", "dim_location.parquet")
        records = [{"location_key": 0, "region": "UNKNOWN", "region_group": "UNKNOWN"}]

        curr_key = 1
        for reg in region_list:
            r_str = str(reg).strip()
            if r_str and r_str != "N/A":
                group = "Americas" if "us" in r_str or "ca" in r_str else "EMEA" if "eu" in r_str else "APAC"
                records.append({
                    "location_key": curr_key,
                    "region": r_str,
                    "region_group": group
                })
                curr_key += 1

        df = pd.DataFrame(records).drop_duplicates(subset=['region'])
        df.to_parquet(path, index=False)
        return df

    @classmethod
    def _populate_dim_service(cls, service_df) -> pd.DataFrame:
        path = os.path.join(WAREHOUSE_DIR, "dim_service", "dim_service.parquet")
        records = [{"service_key": 0, "service": "UNKNOWN", "resource_type": "UNKNOWN"}]

        curr_key = 1
        for _, r in service_df.iterrows():
            svc = str(r['service']).strip()
            res_t = str(r['resource_type']).strip()
            records.append({
                "service_key": curr_key,
                "service": svc,
                "resource_type": res_t
            })
            curr_key += 1

        df = pd.DataFrame(records).drop_duplicates(subset=['service', 'resource_type'])
        df.to_parquet(path, index=False)
        return df

    @classmethod
    def _populate_dim_environment(cls, env_list) -> pd.DataFrame:
        path = os.path.join(WAREHOUSE_DIR, "dim_environment", "dim_environment.parquet")
        records = [{"environment_key": 0, "environment": "UNKNOWN"}]

        curr_key = 1
        for env in env_list:
            e_str = str(env).strip().lower()
            if e_str and e_str != "n/a":
                records.append({"environment_key": curr_key, "environment": e_str})
                curr_key += 1

        df = pd.DataFrame(records).drop_duplicates(subset=['environment'])
        df.to_parquet(path, index=False)
        return df

    @classmethod
    def _populate_dim_currency(cls, curr_list) -> pd.DataFrame:
        path = os.path.join(WAREHOUSE_DIR, "dim_currency", "dim_currency.parquet")
        records = [{"currency_key": 0, "currency": "UNKNOWN"}, {"currency_key": 1, "currency": "USD"}]

        df = pd.DataFrame(records).drop_duplicates(subset=['currency'])
        df.to_parquet(path, index=False)
        return df

    # -------------------------------------------------------------------------
    # ANALYTICAL VIEWS & QUERY EXECUTION ENGINE
    # -------------------------------------------------------------------------

    @classmethod
    def refresh_analytical_views(cls):
        """Builds analytical summary cache tables representing SQL views."""
        fact_path = os.path.join(WAREHOUSE_DIR, "fact_cloud_cost", "fact_cloud_cost.parquet")
        if not os.path.exists(fact_path):
            return

        df_fact = pd.read_parquet(fact_path)
        dim_cloud = pd.read_parquet(os.path.join(WAREHOUSE_DIR, "dim_cloud", "dim_cloud.parquet"))
        dim_org = pd.read_parquet(os.path.join(WAREHOUSE_DIR, "dim_organization", "dim_organization.parquet"))
        dim_service = pd.read_parquet(os.path.join(WAREHOUSE_DIR, "dim_service", "dim_service.parquet"))
        dim_date = pd.read_parquet(os.path.join(WAREHOUSE_DIR, "dim_date", "dim_date.parquet"))

        # Merge Fact with Dimensions
        df_joined = df_fact.merge(dim_cloud, on='cloud_key', how='left')
        df_joined = df_joined.merge(dim_org, on='organization_key', how='left')
        df_joined = df_joined.merge(dim_service, on='service_key', how='left')
        df_joined = df_joined.merge(dim_date, on='date_key', how='left')

        # Save View: vw_provider_cost
        vw_provider = df_joined.groupby(['cloud_provider', 'provider_group'], as_index=False).agg(
            total_net_cost=('net_cost', 'sum'),
            total_list_cost=('list_cost', 'sum'),
            total_savings=('total_savings', 'sum'),
            record_count=('net_cost', 'count')
        )
        vw_provider.to_json(os.path.join(WAREHOUSE_DIR, "vw_provider_cost.json"), orient="records")

        # Save View: vw_department_cost
        vw_dept = df_joined.groupby(['department', 'business_unit'], as_index=False).agg(
            total_net_cost=('net_cost', 'sum'),
            total_budget=('budget_amount', 'sum'),
            total_savings=('total_savings', 'sum'),
            record_count=('net_cost', 'count')
        )
        vw_dept.to_json(os.path.join(WAREHOUSE_DIR, "vw_department_cost.json"), orient="records")

        # Save View: vw_service_cost
        vw_svc = df_joined.groupby(['service', 'resource_type'], as_index=False).agg(
            total_net_cost=('net_cost', 'sum'),
            total_list_cost=('list_cost', 'sum'),
            record_count=('net_cost', 'count')
        )
        vw_svc.to_json(os.path.join(WAREHOUSE_DIR, "vw_service_cost.json"), orient="records")

    @classmethod
    def get_warehouse_summary(cls) -> Dict[str, Any]:
        """Returns warehouse metrics, schema metadata, and table row counts."""
        fact_path = os.path.join(WAREHOUSE_DIR, "fact_cloud_cost", "fact_cloud_cost.parquet")
        fact_rows = 0
        total_net_cost = 0.0
        total_savings = 0.0
        anomalies_count = 0

        if os.path.exists(fact_path):
            df_fact = pd.read_parquet(fact_path)
            fact_rows = len(df_fact)
            total_net_cost = float(df_fact['net_cost'].sum())
            total_savings = float(df_fact['total_savings'].sum())
            anomalies_count = int((df_fact['is_anomaly'] == True).sum())

        dim_counts = {}
        for dim in [
            "dim_date", "dim_cloud", "dim_account", "dim_project",
            "dim_organization", "dim_business_unit", "dim_department", "dim_cost_center",
            "dim_location", "dim_service", "dim_environment", "dim_currency"
        ]:
            dim_p = os.path.join(WAREHOUSE_DIR, dim, f"{dim}.parquet")
            if os.path.exists(dim_p):
                dim_counts[dim] = len(pd.read_parquet(dim_p))
            else:
                dim_counts[dim] = 0

        return {
            "status": "HEALTHY",
            "catalog": "cloud_cost_catalog",
            "schema": "cloud_warehouse",
            "storage_format": "Databricks Delta Lake / Parquet",
            "fact_table": "fact_cloud_cost",
            "fact_record_count": fact_rows,
            "total_net_cost_usd": round(total_net_cost, 2),
            "total_savings_usd": round(total_savings, 2),
            "anomalies_count": anomalies_count,
            "dimension_counts": dim_counts,
            "grain": "ONE ROW PER DATE, ACCOUNT, PROJECT, ENVIRONMENT, PROVIDER, REGION, SERVICE, AND RESOURCE TYPE"
        }

    @classmethod
    def execute_analytical_queries(cls) -> List[Dict[str, Any]]:
        """Executes the 15 required warehouse analytical queries against the warehouse model."""
        fact_path = os.path.join(WAREHOUSE_DIR, "fact_cloud_cost", "fact_cloud_cost.parquet")
        if not os.path.exists(fact_path):
            return []

        df_fact = pd.read_parquet(fact_path)
        dim_cloud = pd.read_parquet(os.path.join(WAREHOUSE_DIR, "dim_cloud", "dim_cloud.parquet"))
        dim_org = pd.read_parquet(os.path.join(WAREHOUSE_DIR, "dim_organization", "dim_organization.parquet"))
        dim_service = pd.read_parquet(os.path.join(WAREHOUSE_DIR, "dim_service", "dim_service.parquet"))
        dim_date = pd.read_parquet(os.path.join(WAREHOUSE_DIR, "dim_date", "dim_date.parquet"))
        dim_env = pd.read_parquet(os.path.join(WAREHOUSE_DIR, "dim_environment", "dim_environment.parquet"))
        dim_loc = pd.read_parquet(os.path.join(WAREHOUSE_DIR, "dim_location", "dim_location.parquet"))
        dim_proj = pd.read_parquet(os.path.join(WAREHOUSE_DIR, "dim_project", "dim_project.parquet"))

        df_joined = df_fact.merge(dim_cloud, on='cloud_key', how='left')
        df_joined = df_joined.merge(dim_org, on='organization_key', how='left')
        df_joined = df_joined.merge(dim_service, on='service_key', how='left')
        df_joined = df_joined.merge(dim_date, on='date_key', how='left')
        df_joined = df_joined.merge(dim_env, on='environment_key', how='left')
        df_joined = df_joined.merge(dim_loc, on='location_key', how='left')
        df_joined = df_joined.merge(dim_proj.rename(columns={'environment': 'proj_environment'}), on='project_key', how='left')

        # Standardize environment column name if suffix added
        if 'environment' not in df_joined.columns and 'environment_x' in df_joined.columns:
            df_joined['environment'] = df_joined['environment_x']

        queries = []

        # 1. Total Cloud Cost
        queries.append({
            "id": 1, "title": "1. Total Cloud Cost",
            "sql": "SELECT SUM(net_cost) AS total_cost FROM fact_cloud_cost;",
            "result": [{"total_cost": round(df_joined['net_cost'].sum(), 2)}]
        })

        # 2. Cost by Provider
        queries.append({
            "id": 2, "title": "2. Cost by Cloud Provider",
            "sql": "SELECT cloud_provider, SUM(net_cost) FROM fact_cloud_cost JOIN dim_cloud GROUP BY cloud_provider;",
            "result": df_joined.groupby('cloud_provider')['net_cost'].sum().round(2).reset_index().to_dict(orient='records')
        })

        # 3. Cost by Month
        queries.append({
            "id": 3, "title": "3. Cost by Month",
            "sql": "SELECT month_name, year, SUM(net_cost) FROM fact_cloud_cost JOIN dim_date GROUP BY year, month_name;",
            "result": df_joined.groupby(['year', 'month_name'])['net_cost'].sum().round(2).reset_index().to_dict(orient='records')
        })

        # 4. Cost by Department
        queries.append({
            "id": 4, "title": "4. Cost by Department",
            "sql": "SELECT department, SUM(net_cost) FROM fact_cloud_cost JOIN dim_organization GROUP BY department;",
            "result": df_joined.groupby('department')['net_cost'].sum().round(2).reset_index().to_dict(orient='records')
        })

        # 5. Cost by Service
        queries.append({
            "id": 5, "title": "5. Cost by Service",
            "sql": "SELECT service, SUM(net_cost) FROM fact_cloud_cost JOIN dim_service GROUP BY service;",
            "result": df_joined.groupby('service')['net_cost'].sum().round(2).reset_index().to_dict(orient='records')
        })

        # 6. Cost by Region
        queries.append({
            "id": 6, "title": "6. Cost by Region",
            "sql": "SELECT region, SUM(net_cost) FROM fact_cloud_cost JOIN dim_location GROUP BY region;",
            "result": df_joined.groupby('region')['net_cost'].sum().round(2).reset_index().to_dict(orient='records')
        })

        # 7. Cost by Environment
        queries.append({
            "id": 7, "title": "7. Cost by Environment",
            "sql": "SELECT environment, SUM(net_cost) FROM fact_cloud_cost JOIN dim_environment GROUP BY environment;",
            "result": df_joined.groupby('environment')['net_cost'].sum().round(2).reset_index().to_dict(orient='records')
        })

        # 8. Budget Utilization
        queries.append({
            "id": 8, "title": "8. Budget Utilization Overview",
            "sql": "SELECT department, AVG(budget_utilization_pct) AS avg_utilization FROM fact_cloud_cost JOIN dim_organization GROUP BY department;",
            "result": df_joined.groupby('department')['budget_utilization_pct'].mean().round(2).reset_index().to_dict(orient='records')
        })

        # 9. Total Savings
        queries.append({
            "id": 9, "title": "9. Total Savings & Discounts",
            "sql": "SELECT SUM(total_savings) AS total_savings, AVG(effective_discount_pct) AS avg_discount FROM fact_cloud_cost;",
            "result": [{"total_savings": round(df_joined['total_savings'].sum(), 2), "avg_discount_pct": round(df_joined['effective_discount_pct'].mean(), 2)}]
        })

        # 10. Top Expensive Projects
        queries.append({
            "id": 10, "title": "10. Top Expensive Projects",
            "sql": "SELECT project_id, SUM(net_cost) AS total_cost FROM fact_cloud_cost JOIN dim_project GROUP BY project_id ORDER BY total_cost DESC LIMIT 5;",
            "result": df_joined.groupby('project_id')['net_cost'].sum().round(2).reset_index().sort_values(by='net_cost', ascending=False).head(5).to_dict(orient='records')
        })

        # 11. Anomalous Spending
        queries.append({
            "id": 11, "title": "11. Anomalous High-Risk Spend Items",
            "sql": "SELECT service, project_id, net_cost FROM fact_cloud_cost WHERE is_anomaly = true;",
            "result": df_joined[df_joined['is_anomaly'] == True][['service', 'project_id', 'net_cost', 'cost_risk_level']].head(10).to_dict(orient='records')
        })

        # 12. Forecast vs Budget
        queries.append({
            "id": 12, "title": "12. Forecast Variance & Remaining Budget",
            "sql": "SELECT department, SUM(budget_amount) - SUM(net_cost) AS budget_remaining FROM fact_cloud_cost JOIN dim_organization GROUP BY department;",
            "result": df_joined.groupby('department')['budget_remaining'].sum().round(2).reset_index().to_dict(orient='records')
        })

        # 13. Monthly Trend
        queries.append({
            "id": 13, "title": "13. Monthly Spending Trend",
            "sql": "SELECT date, SUM(net_cost) FROM fact_cloud_cost JOIN dim_date GROUP BY date ORDER BY date;",
            "result": df_joined.groupby('date')['net_cost'].sum().round(2).reset_index().to_dict(orient='records')
        })

        # 14. Provider vs Department
        queries.append({
            "id": 14, "title": "14. Provider vs Department Breakdown",
            "sql": "SELECT cloud_provider, department, SUM(net_cost) FROM fact_cloud_cost JOIN dim_cloud JOIN dim_organization GROUP BY cloud_provider, department;",
            "result": df_joined.groupby(['cloud_provider', 'department'])['net_cost'].sum().round(2).reset_index().to_dict(orient='records')
        })

        # 15. Service vs Resource Type
        queries.append({
            "id": 15, "title": "15. Service vs Resource Type Breakdown",
            "sql": "SELECT service, resource_type, SUM(net_cost) FROM fact_cloud_cost JOIN dim_service GROUP BY service, resource_type;",
            "result": df_joined.groupby(['service', 'resource_type'])['net_cost'].sum().round(2).reset_index().to_dict(orient='records')
        })

        cleaned_queries = []
        for query in queries:
            cleaned_query = dict(query)
            cleaned_query['result'] = cls._json_safe(query.get('result'))
            cleaned_queries.append(cleaned_query)
        return cleaned_queries

    # -------------------------------------------------------------------------
    # SLOWLY CHANGING DIMENSION (SCD TYPE 2) SIMULATION DEMO
    # -------------------------------------------------------------------------

    @classmethod
    def simulate_scd2_update(cls, project_id: str, new_environment: str) -> Dict[str, Any]:
        """
        Demonstrates SCD Type 2 logic on dim_project:
        - Sets is_current = False & expiry_date = today on existing record.
        - Inserts new record with is_current = True & effective_date = today.
        """
        path = os.path.join(WAREHOUSE_DIR, "dim_project", "dim_project.parquet")
        if not os.path.exists(path):
            return {"status": "ERROR", "message": "dim_project table does not exist yet."}

        df = pd.read_parquet(path)
        today_str = datetime.utcnow().strftime('%Y-%m-%d')

        # Find active record for project_id
        active_mask = (df['project_id'] == project_id) & (df['is_current'] == True)
        if not active_mask.any():
            return {"status": "NOT_FOUND", "message": f"No active project found with ID '{project_id}'"}

        # Expire old record
        df.loc[active_mask, 'is_current'] = False
        df.loc[active_mask, 'expiry_date'] = today_str

        # Add new version
        new_key = df['project_key'].max() + 1
        new_record = {
            "project_key": new_key,
            "project_id": project_id,
            "account_key": 1,
            "environment": new_environment,
            "effective_date": today_str,
            "expiry_date": None,
            "is_current": True
        }
        df = pd.concat([df, pd.DataFrame([new_record])], ignore_index=True)
        df.to_parquet(path, index=False)

        logger.info(f"[WAREHOUSE:SCD2] Updated project '{project_id}' via SCD Type 2 to environment '{new_environment}'")
        return {
            "status": "SUCCESS",
            "project_id": project_id,
            "new_project_key": new_key,
            "new_environment": new_environment,
            "effective_date": today_str,
            "scd_type": "SCD Type 2"
        }
