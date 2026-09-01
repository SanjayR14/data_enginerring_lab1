"""
Cloud Cost Intelligence & Data Engineering Platform
Phase 6: Data Cube, OLAP Engine, Multidimensional Aggregations & Business Interpretation
Namespace: cloud_cost_catalog.cloud_warehouse.olap
"""

import os
import json
import logging
import pandas as pd
import numpy as np
from typing import Dict, Any, List, Optional, Union

logger = logging.getLogger(__name__)

WAREHOUSE_DIR = "./data/delta/warehouse"
OLAP_AGG_DIR = "./data/delta/olap_aggregates"

ALLOWED_DIMENSIONS = {
    "time": ["year", "quarter", "month", "month_name", "day", "date"],
    "date": ["year", "quarter", "month", "month_name", "day", "date"],
    "cloud_provider": ["cloud_provider"],
    "provider": ["cloud_provider"],
    "account": ["account_id"],
    "project": ["project_id"],
    "organization": ["business_unit", "department", "cost_center"],
    "business_unit": ["business_unit"],
    "department": ["department"],
    "cost_center": ["cost_center"],
    "region": ["region"],
    "location": ["region"],
    "service": ["service"],
    "resource_type": ["resource_type"],
    "environment": ["environment"],
    "currency": ["currency"]
}

ALLOWED_MEASURES = [
    "net_cost", "list_cost", "usage_quantity", "discount_amount",
    "reserved_savings", "savings_plan_savings", "spot_savings",
    "total_savings", "budget_amount", "budget_remaining",
    "budget_utilization_pct", "effective_discount_pct",
    "forecast_monthly_cost", "anomaly_score", "is_anomaly", "record_count"
]

DIMENSION_MAPPINGS = {
    "time": "month_name",
    "date": "date",
    "year": "year",
    "quarter": "quarter",
    "month": "month_name",
    "day": "date",
    "cloud_provider": "cloud_provider",
    "provider": "cloud_provider",
    "account": "account_id",
    "account_id": "account_id",
    "project": "project_id",
    "project_id": "project_id",
    "business_unit": "business_unit",
    "department": "department",
    "cost_center": "cost_center",
    "region": "region",
    "location": "region",
    "service": "service",
    "resource_type": "resource_type",
    "environment": "environment",
    "currency": "currency"
}


class OLAPEngine:

    @classmethod
    def _ensure_directories(cls):
        os.makedirs(OLAP_AGG_DIR, exist_ok=True)

    @classmethod
    def get_cube_dataset(cls) -> pd.DataFrame:
        """
        Loads and joins Fact and Dimension tables from Data Warehouse to construct base Data Cube.
        Base Grain: Date x Cloud Provider x Account x Project x Organization x Region x Service x Resource Type x Environment
        """
        fact_path = os.path.join(WAREHOUSE_DIR, "fact_cloud_cost", "fact_cloud_cost.parquet")
        if not os.path.exists(fact_path):
            # Create synthetic fallback dataset if warehouse fact table doesn't exist yet
            return cls._generate_mock_cube_dataset()

        try:
            df_fact = pd.read_parquet(fact_path)
            dim_cloud = pd.read_parquet(os.path.join(WAREHOUSE_DIR, "dim_cloud", "dim_cloud.parquet"))
            dim_org = pd.read_parquet(os.path.join(WAREHOUSE_DIR, "dim_organization", "dim_organization.parquet"))
            dim_service = pd.read_parquet(os.path.join(WAREHOUSE_DIR, "dim_service", "dim_service.parquet"))
            dim_date = pd.read_parquet(os.path.join(WAREHOUSE_DIR, "dim_date", "dim_date.parquet"))
            dim_env = pd.read_parquet(os.path.join(WAREHOUSE_DIR, "dim_environment", "dim_environment.parquet"))
            dim_loc = pd.read_parquet(os.path.join(WAREHOUSE_DIR, "dim_location", "dim_location.parquet"))
            dim_proj = pd.read_parquet(os.path.join(WAREHOUSE_DIR, "dim_project", "dim_project.parquet"))
            dim_acct = pd.read_parquet(os.path.join(WAREHOUSE_DIR, "dim_account", "dim_account.parquet"))

            df_joined = df_fact.merge(dim_cloud, on='cloud_key', how='left')
            df_joined = df_joined.merge(dim_org, on='organization_key', how='left')
            df_joined = df_joined.merge(dim_service, on='service_key', how='left')
            df_joined = df_joined.merge(dim_date, on='date_key', how='left')
            df_joined = df_joined.merge(dim_env, on='environment_key', how='left')
            df_joined = df_joined.merge(dim_loc, on='location_key', how='left')
            df_joined = df_joined.merge(dim_proj.drop(columns=['environment', 'cloud_provider', 'account_key'], errors='ignore'), on='project_key', how='left')
            df_joined = df_joined.merge(dim_acct.drop(columns=['cloud_key', 'cloud_provider'], errors='ignore'), on='account_key', how='left')

            # Ensure environment column name is present
            if 'environment' not in df_joined.columns and 'environment_x' in df_joined.columns:
                df_joined['environment'] = df_joined['environment_x']

            # Forecast monthly cost approximation (current cost * 1.1)
            df_joined['forecast_monthly_cost'] = (df_joined['net_cost'] * 1.1).round(2)
            df_joined['anomaly_score'] = np.where(df_joined['is_anomaly'], 0.85, 0.15)
            df_joined['record_count'] = 1

            return df_joined
        except Exception as e:
            logger.error(f"[OLAP] Error loading warehouse cube dataset: {e}")
            return cls._generate_mock_cube_dataset()

    @classmethod
    def _generate_mock_cube_dataset(cls) -> pd.DataFrame:
        """Generates fallback sample dataset for OLAP testing when warehouse is not pre-populated."""
        records = []
        providers = ['AWS', 'Azure', 'GCP']
        depts = ['Engineering', 'Finance', 'Marketing', 'Product', 'HR']
        envs = ['production', 'staging', 'development']
        services = ['Compute', 'Storage', 'Database', 'Networking', 'AI/ML']
        regions = ['us-east-1', 'us-west-2', 'eu-west-1', 'ap-southeast-1']
        
        dates = pd.date_range('2023-01-01', '2023-12-31', freq='D')
        
        np.random.seed(42)
        for i in range(100):
            d = pd.to_datetime(np.random.choice(dates))
            p = str(np.random.choice(providers))
            dept = str(np.random.choice(depts))
            env = str(np.random.choice(envs))
            srv = str(np.random.choice(services))
            reg = str(np.random.choice(regions))
            net_cost = round(float(np.random.uniform(100, 2500)), 2)
            list_cost = round(net_cost * 1.2, 2)
            budget = round(float(np.random.uniform(1000, 3000)), 2)
            is_anom = bool(net_cost > 2000)
            
            records.append({
                'date': d.strftime('%Y-%m-%d'),
                'year': d.year,
                'quarter': f"Q{(d.month-1)//3 + 1}",
                'month': d.month,
                'month_name': d.strftime('%B'),
                'cloud_provider': p,
                'account_id': f"acc-{p.lower()}-01",
                'project_id': f"prj-{dept.lower()}-01",
                'business_unit': f"BU-{dept}",
                'department': dept,
                'cost_center': f"CC-{dept[:3].upper()}-100",
                'region': reg,
                'service': srv,
                'resource_type': f"{srv}_instance",
                'environment': env,
                'currency': 'USD',
                'usage_quantity': round(float(np.random.uniform(50, 500)), 1),
                'list_cost': list_cost,
                'discount_amount': round(list_cost - net_cost, 2),
                'net_cost': net_cost,
                'reserved_savings': round(net_cost * 0.1, 2),
                'savings_plan_savings': round(net_cost * 0.05, 2),
                'spot_savings': round(net_cost * 0.05, 2),
                'total_savings': round(net_cost * 0.2, 2),
                'budget_amount': budget,
                'budget_remaining': round(budget - net_cost, 2),
                'budget_utilization_pct': round((net_cost / budget) * 100.0, 2),
                'effective_discount_pct': 16.67,
                'forecast_monthly_cost': round(net_cost * 1.1, 2),
                'is_anomaly': is_anom,
                'anomaly_score': 0.88 if is_anom else 0.12,
                'record_count': 1
            })
        return pd.DataFrame(records)

    @classmethod
    def refresh_olap_aggregates(cls) -> Dict[str, Any]:
        """
        Materializes aggregated OLAP views into Parquet files to accelerate frontend analytical queries.
        - agg_monthly_provider_cost
        - agg_monthly_department_cost
        - agg_monthly_service_cost
        - agg_daily_cloud_cost
        """
        cls._ensure_directories()
        df = cls.get_cube_dataset()

        # 1. Monthly Provider Cost
        m_prov = df.groupby(['year', 'month', 'month_name', 'cloud_provider']).agg({
            'net_cost': 'sum', 'list_cost': 'sum', 'total_savings': 'sum', 'budget_amount': 'sum', 'record_count': 'sum'
        }).reset_index().round(2)
        m_prov.to_parquet(os.path.join(OLAP_AGG_DIR, "agg_monthly_provider_cost.parquet"), index=False)

        # 2. Monthly Department Cost
        m_dept = df.groupby(['year', 'month', 'month_name', 'department']).agg({
            'net_cost': 'sum', 'budget_amount': 'sum', 'budget_remaining': 'sum', 'total_savings': 'sum', 'record_count': 'sum'
        }).reset_index().round(2)
        m_dept.to_parquet(os.path.join(OLAP_AGG_DIR, "agg_monthly_department_cost.parquet"), index=False)

        # 3. Monthly Service Cost
        m_srv = df.groupby(['year', 'month', 'month_name', 'service']).agg({
            'net_cost': 'sum', 'total_savings': 'sum', 'record_count': 'sum'
        }).reset_index().round(2)
        m_srv.to_parquet(os.path.join(OLAP_AGG_DIR, "agg_monthly_service_cost.parquet"), index=False)

        # 4. Daily Cloud Cost
        d_cloud = df.groupby(['date', 'cloud_provider', 'environment']).agg({
            'net_cost': 'sum', 'total_savings': 'sum', 'is_anomaly': 'sum', 'record_count': 'sum'
        }).reset_index().round(2)
        d_cloud.to_parquet(os.path.join(OLAP_AGG_DIR, "agg_daily_cloud_cost.parquet"), index=False)

        return {
            "status": "SUCCESS",
            "message": "OLAP aggregate materialized views refreshed successfully.",
            "aggregates": [
                "agg_monthly_provider_cost", "agg_monthly_department_cost",
                "agg_monthly_service_cost", "agg_daily_cloud_cost"
            ]
        }

    @classmethod
    def get_metadata(cls) -> Dict[str, Any]:
        """Returns cube metadata, hierarchies, dimensions, and supported OLAP operations."""
        return {
            "cube_name": "cloud_cost_cube",
            "catalog": "cloud_cost_catalog.cloud_warehouse.olap",
            "grain": "Date x Cloud Provider x Account x Project x Organization x Region x Service x Resource Type x Environment",
            "dimensions": [
                {"name": "Time", "key": "time", "levels": ["Year", "Quarter", "Month", "Day"]},
                {"name": "Cloud Provider", "key": "cloud_provider", "levels": ["Cloud Provider", "Account", "Project"]},
                {"name": "Organization", "key": "organization", "levels": ["Business Unit", "Department", "Cost Center"]},
                {"name": "Resource", "key": "resource", "levels": ["Service", "Resource Type"]},
                {"name": "Geography", "key": "region", "levels": ["Region"]},
                {"name": "Environment", "key": "environment", "levels": ["Environment"]},
                {"name": "Currency", "key": "currency", "levels": ["Currency"]}
            ],
            "hierarchies": {
                "TIME": ["year", "quarter", "month", "date"],
                "CLOUD": ["cloud_provider", "account_id", "project_id"],
                "ORGANIZATION": ["business_unit", "department", "cost_center"],
                "RESOURCE": ["service", "resource_type"],
                "GEOGRAPHY": ["region"],
                "ENVIRONMENT": ["environment"]
            },
            "measures": [
                {"key": "net_cost", "label": "Net Cost ($)", "agg": "SUM"},
                {"key": "list_cost", "label": "List Cost ($)", "agg": "SUM"},
                {"key": "usage_quantity", "label": "Usage Quantity", "agg": "SUM"},
                {"key": "discount_amount", "label": "Discount Amount ($)", "agg": "SUM"},
                {"key": "total_savings", "label": "Total Savings ($)", "agg": "SUM"},
                {"key": "budget_amount", "label": "Allocated Budget ($)", "agg": "SUM"},
                {"key": "budget_utilization_pct", "label": "Avg Budget Utilization (%)", "agg": "AVG"},
                {"key": "forecast_monthly_cost", "label": "Forecast Monthly Cost ($)", "agg": "SUM"},
                {"key": "anomaly_score", "label": "Avg Anomaly Score", "agg": "AVG"},
                {"key": "anomaly_count", "label": "Anomalies Count", "agg": "COUNT"}
            ],
            "operations": ["rollup", "drilldown", "slice", "dice", "pivot", "top_n", "time_series"],
            "security": {
                "sql_injection_protection": True,
                "whitelist_validation": True
            }
        }

    @classmethod
    def _apply_filters(cls, df: pd.DataFrame, filters: Optional[Dict[str, Any]]) -> pd.DataFrame:
        """Applies validated filter conditions safely without dynamic SQL string concatenation."""
        if not filters:
            return df
        
        filtered_df = df.copy()
        for k, v in filters.items():
            col = DIMENSION_MAPPINGS.get(k.lower(), k)
            if col in filtered_df.columns and v is not None and v != "" and v != "All" and v != ["All"]:
                if isinstance(v, list):
                    filtered_df = filtered_df[filtered_df[col].isin(v)]
                else:
                    filtered_df = filtered_df[filtered_df[col] == v]
        return filtered_df

    @classmethod
    def rollup(cls, dimension: str, level: str, measure: str = "net_cost", filters: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Roll-up Operation: Move up analytical hierarchy (e.g., Day -> Month, Month -> Quarter, Project -> Account -> Provider).
        """
        cls._validate_params(dimension=dimension, measure=measure)
        df = cls.get_cube_dataset()
        df = cls._apply_filters(df, filters)

        group_col = DIMENSION_MAPPINGS.get(level.lower(), level)
        if group_col not in df.columns:
            group_col = DIMENSION_MAPPINGS.get(dimension.lower(), "cloud_provider")

        if group_col not in df.columns:
            raise ValueError(f"Invalid target roll-up level/dimension: {level}")

        agg_func = "mean" if "pct" in measure or "score" in measure or "utilization" in measure else "sum"
        result_df = df.groupby(group_col)[measure].agg(agg_func).round(2).reset_index()
        
        # Sort chronologically if time-based
        if group_col in ['month_name', 'month']:
            month_order = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December']
            result_df['sort_idx'] = result_df[group_col].apply(lambda x: month_order.index(x) if x in month_order else 99)
            result_df = result_df.sort_values('sort_idx').drop(columns=['sort_idx'])

        data = result_df.to_dict(orient="records")
        interpretation = cls.generate_rule_based_interpretation("rollup", data, {"dimension": dimension, "level": level, "measure": measure})

        return {
            "operation": "ROLLUP",
            "dimension": dimension,
            "target_level": level,
            "measure": measure,
            "record_count": len(data),
            "data": data,
            "interpretation": interpretation
        }

    @classmethod
    def drilldown(cls, hierarchy: str, current_level: str, next_level: str, filters: Optional[Dict[str, Any]] = None, measure: str = "net_cost") -> Dict[str, Any]:
        """
        Drill-down Operation: Decompose summary metric into lower level granular detail.
        e.g., Year 2023 -> Quarters Q1, Q2, Q3, Q4 -> Months -> Days
        """
        cls._validate_params(measure=measure)
        df = cls.get_cube_dataset()
        df = cls._apply_filters(df, filters)

        next_col = DIMENSION_MAPPINGS.get(next_level.lower(), next_level)
        if next_col not in df.columns:
            next_col = "month_name" if hierarchy.upper() == "TIME" else "account_id"

        agg_func = "mean" if "pct" in measure or "score" in measure else "sum"
        result_df = df.groupby(next_col)[measure].agg(agg_func).round(2).reset_index()

        data = result_df.to_dict(orient="records")
        interpretation = cls.generate_rule_based_interpretation("drilldown", data, {"hierarchy": hierarchy, "current": current_level, "next": next_level, "measure": measure})

        return {
            "operation": "DRILLDOWN",
            "hierarchy": hierarchy,
            "current_level": current_level,
            "next_level": next_level,
            "measure": measure,
            "record_count": len(data),
            "data": data,
            "interpretation": interpretation
        }

    @classmethod
    def slice(cls, dimension: str, value: Any, measure: str = "net_cost") -> Dict[str, Any]:
        """
        Slice Operation: Fixes one dimension to a single specific value (e.g., Cloud Provider = AWS).
        """
        cls._validate_params(dimension=dimension, measure=measure)
        df = cls.get_cube_dataset()
        dim_col = DIMENSION_MAPPINGS.get(dimension.lower(), dimension)

        if dim_col not in df.columns:
            raise ValueError(f"Dimension '{dimension}' not found in Data Cube.")

        sliced_df = df[df[dim_col] == value]
        
        # Breakdown by department for the sliced dimension
        breakdown_col = "department" if dim_col != "department" else "service"
        agg_func = "mean" if "pct" in measure or "score" in measure else "sum"
        
        res_df = sliced_df.groupby(breakdown_col)[measure].agg(agg_func).round(2).reset_index()
        data = res_df.to_dict(orient="records")

        total_val = round(float(sliced_df[measure].sum() if agg_func == "sum" else sliced_df[measure].mean()), 2)
        interpretation = f"Sliced Cube by {dimension} = '{value}'. Total {measure} across all records is ${total_val:,.2f}."

        return {
            "operation": "SLICE",
            "dimension": dimension,
            "value": value,
            "measure": measure,
            "total_value": total_val,
            "record_count": len(sliced_df),
            "data": data,
            "interpretation": interpretation
        }

    @classmethod
    def dice(cls, filters: Dict[str, Any], measures: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Dice Operation: Selects a sub-cube by applying multiple dimension filters simultaneously.
        """
        if not measures:
            measures = ["net_cost", "budget_amount", "total_savings"]

        df = cls.get_cube_dataset()
        diced_df = cls._apply_filters(df, filters)

        summary = {}
        for m in measures:
            if m in diced_df.columns:
                if "pct" in m or "score" in m:
                    summary[m] = round(float(diced_df[m].mean()), 2)
                else:
                    summary[m] = round(float(diced_df[m].sum()), 2)

        # Department breakdown within diced sub-cube
        dept_summary = diced_df.groupby("department")[measures[0]].sum().round(2).reset_index().to_dict(orient="records")

        interpretation = f"Diced sub-cube matching {len(filters)} dimension constraints returned {len(diced_df)} records. Net Cost: ${summary.get('net_cost', 0):,.2f}."

        return {
            "operation": "DICE",
            "applied_filters": filters,
            "measures_summary": summary,
            "matching_records": len(diced_df),
            "data": dept_summary,
            "interpretation": interpretation
        }

    @classmethod
    def pivot(cls, rows: str, columns: str, measure: str = "net_cost", filters: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Pivot Operation: Rotates data axes to display multidimensional summary matrix (e.g. Rows: Department, Columns: Cloud Provider).
        """
        cls._validate_params(dimension=rows, measure=measure)
        cls._validate_params(dimension=columns, measure=measure)

        df = cls.get_cube_dataset()
        df = cls._apply_filters(df, filters)

        row_col = DIMENSION_MAPPINGS.get(rows.lower(), rows)
        col_col = DIMENSION_MAPPINGS.get(columns.lower(), columns)

        agg_func = "mean" if "pct" in measure or "score" in measure else "sum"
        pivot_df = pd.pivot_table(df, values=measure, index=row_col, columns=col_col, aggfunc=agg_func, fill_value=0.0).round(2)

        matrix_records = pivot_df.reset_index().to_dict(orient="records")
        col_names = [str(c) for c in pivot_df.columns]

        interpretation = f"Pivot Matrix generated for {rows} (Rows) x {columns} (Columns) evaluating {measure}."

        return {
            "operation": "PIVOT",
            "rows_dimension": rows,
            "cols_dimension": columns,
            "measure": measure,
            "columns": col_names,
            "matrix": matrix_records,
            "interpretation": interpretation
        }

    @classmethod
    def get_top_n(cls, category: str = "projects", n: int = 10, measure: str = "net_cost", filters: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Top-N Analysis: Returns top ranking entities by cost/measure along with percentage contribution.
        """
        cls._validate_params(measure=measure)
        df = cls.get_cube_dataset()
        df = cls._apply_filters(df, filters)

        col_map = {
            "projects": "project_id",
            "services": "service",
            "departments": "department",
            "regions": "region",
            "accounts": "account_id",
            "anomalies": "project_id"
        }
        group_col = col_map.get(category.lower(), "project_id")

        if category == "anomalies":
            df = df[df['is_anomaly'] == True]

        grouped = df.groupby(group_col)[measure].sum().round(2).reset_index()
        total = grouped[measure].sum()
        grouped['pct_of_total'] = np.where(total > 0, (grouped[measure] / total) * 100.0, 0.0).round(2)
        top_df = grouped.sort_values(by=measure, ascending=False).head(n)

        data = top_df.to_dict(orient="records")
        top_name = data[0][group_col] if data else "N/A"
        top_val = data[0][measure] if data else 0

        interpretation = f"Top {category.capitalize()} analysis: '{top_name}' leads with ${top_val:,.2f} ({data[0]['pct_of_total']}% of total)." if data else "No data."

        return {
            "category": category,
            "top_n": n,
            "measure": measure,
            "total_overall": round(total, 2),
            "data": data,
            "interpretation": interpretation
        }

    @classmethod
    def get_time_series(cls, granularity: str = "monthly", measure: str = "net_cost", filters: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Time-series Analysis: Returns chronological trend data grouped by day, week, month, quarter, or year.
        """
        cls._validate_params(measure=measure)
        df = cls.get_cube_dataset()
        df = cls._apply_filters(df, filters)

        if granularity == "daily":
            res = df.groupby("date")[measure].sum().round(2).reset_index().sort_values("date")
        elif granularity == "yearly":
            res = df.groupby("year")[measure].sum().round(2).reset_index().sort_values("year")
        elif granularity == "quarterly":
            res = df.groupby(["year", "quarter"])[measure].sum().round(2).reset_index().sort_values(["year", "quarter"])
        else:  # monthly default
            res = df.groupby(["year", "month", "month_name"])[measure].sum().round(2).reset_index().sort_values(["year", "month"])

        data = res.to_dict(orient="records")
        interpretation = f"Chronological {granularity} time-series analysis for {measure} over {len(data)} intervals."

        return {
            "granularity": granularity,
            "measure": measure,
            "data": data,
            "interpretation": interpretation
        }

    @classmethod
    def get_budget_analysis(cls, filters: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Budget OLAP Analysis: Total Budget, Actual Cost, Remaining, Utilization %, Forecast Variance, and Risk Classification.
        """
        df = cls.get_cube_dataset()
        df = cls._apply_filters(df, filters)

        tot_budget = round(float(df['budget_amount'].sum()), 2)
        tot_cost = round(float(df['net_cost'].sum()), 2)
        tot_remaining = round(tot_budget - tot_cost, 2)
        avg_util = round((tot_cost / tot_budget * 100.0), 2) if tot_budget > 0 else 0.0
        tot_forecast = round(float(df['forecast_monthly_cost'].sum()), 2)

        # Department breakdown with classification
        dept_bg = df.groupby('department').agg({
            'budget_amount': 'sum',
            'net_cost': 'sum'
        }).reset_index()

        dept_bg['budget_remaining'] = (dept_bg['budget_amount'] - dept_bg['net_cost']).round(2)
        dept_bg['budget_utilization_pct'] = np.where(dept_bg['budget_amount'] > 0, (dept_bg['net_cost'] / dept_bg['budget_amount']) * 100.0, 0.0).round(2)

        def classify_status(u):
            if u > 90.0:
                return "Over Budget"
            elif u >= 75.0:
                return "Near Budget"
            return "Under Budget"

        dept_bg['status'] = dept_bg['budget_utilization_pct'].apply(classify_status)

        status_counts = dept_bg['status'].value_counts().to_dict()

        interpretation = f"Total allocated cloud budget is ${tot_budget:,.2f} with ${tot_cost:,.2f} spent ({avg_util}% utilization). " \
                         f"{status_counts.get('Over Budget', 0)} departments are Over Budget."

        dept_breakdown = dept_bg.to_dict(orient="records")
        return {
            "total_budget": tot_budget,
            "actual_cost": tot_cost,
            "budget_remaining": tot_remaining,
            "budget_utilization_pct": avg_util,
            "forecast_monthly_cost": tot_forecast,
            "forecast_variance": round(tot_forecast - tot_budget, 2),
            "status_summary": status_counts,
            "data": dept_breakdown,
            "department_budget_breakdown": dept_breakdown,
            "interpretation": interpretation
        }

    @classmethod
    def get_savings_analysis(cls, filters: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Savings & Discounts OLAP Analysis: Reserved, Savings Plan, Spot, Total Savings, Effective Discount %.
        """
        df = cls.get_cube_dataset()
        df = cls._apply_filters(df, filters)

        res_sav = round(float(df['reserved_savings'].sum()), 2)
        sp_sav = round(float(df['savings_plan_savings'].sum()), 2)
        spot_sav = round(float(df['spot_savings'].sum()), 2)
        tot_sav = round(float(df['total_savings'].sum()), 2)
        avg_disc = round(float(df['effective_discount_pct'].mean()), 2)

        prov_sav = df.groupby('cloud_provider').agg({
            'reserved_savings': 'sum',
            'savings_plan_savings': 'sum',
            'spot_savings': 'sum',
            'total_savings': 'sum'
        }).reset_index().round(2).to_dict(orient='records')

        dept_sav = df.groupby('department')['total_savings'].sum().round(2).reset_index().to_dict(orient='records')

        interpretation = f"Cloud cost optimization generated ${tot_sav:,.2f} in total savings with an effective discount rate of {avg_disc}%."

        return {
            "reserved_savings": res_sav,
            "savings_plan_savings": sp_sav,
            "spot_savings": spot_sav,
            "total_savings": tot_sav,
            "effective_discount_pct": avg_disc,
            "data": dept_sav,
            "interpretation": interpretation
        }

    @classmethod
    def get_anomaly_analysis(cls, filters: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Anomaly OLAP Analysis: Anomaly rate %, scores, and concentration breakdown by Provider, Service, Department.
        """
        df = cls.get_cube_dataset()
        df = cls._apply_filters(df, filters)

        tot_records = len(df)
        anom_df = df[df['is_anomaly'] == True]
        anom_records = len(anom_df)
        anom_rate = round((anom_records / tot_records * 100.0), 2) if tot_records > 0 else 0.0
        avg_score = round(float(df['anomaly_score'].mean()), 2)

        by_provider = df.groupby('cloud_provider')['is_anomaly'].sum().to_dict()
        by_service = df.groupby('service')['is_anomaly'].sum().to_dict()
        by_dept = df.groupby('department')['is_anomaly'].sum().to_dict()

        interpretation = f"Anomaly Engine flagged {anom_records} records out of {tot_records} ({anom_rate}% anomaly rate)."

        return {
            "total_records": tot_records,
            "anomalous_records": anom_records,
            "anomaly_rate_pct": anom_rate,
            "average_anomaly_score": avg_score,
            "anomalies_by_provider": by_provider,
            "anomalies_by_service": by_service,
            "anomalies_by_department": by_dept,
            "data": anom_df[['service', 'project_id', 'department', 'net_cost', 'anomaly_score']].head(10).to_dict(orient='records'),
            "interpretation": interpretation
        }

    @classmethod
    def get_comparison_analysis(cls, dimension: str = "cloud_provider", values: Optional[List[str]] = None, measure: str = "net_cost", filters: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Comparative Analysis: Side-by-side metric comparison (e.g., AWS vs Azure vs GCP, Prod vs Staging).
        """
        cls._validate_params(dimension=dimension, measure=measure)
        df = cls.get_cube_dataset()
        df = cls._apply_filters(df, filters)

        dim_col = DIMENSION_MAPPINGS.get(dimension.lower(), dimension)
        if values:
            df = df[df[dim_col].isin(values)]

        comp_df = df.groupby(dim_col).agg({
            'net_cost': 'sum',
            'budget_amount': 'sum',
            'total_savings': 'sum',
            'is_anomaly': 'sum'
        }).reset_index().round(2)

        comp_df['budget_variance'] = (comp_df['budget_amount'] - comp_df['net_cost']).round(2)
        data = comp_df.to_dict(orient="records")

        interpretation = f"Comparative Analysis across {len(data)} {dimension} entities for Net Cost, Budget, and Savings."

        return {
            "dimension": dimension,
            "compared_values": values,
            "measure": measure,
            "data": data,
            "interpretation": interpretation
        }

    @classmethod
    def generate_rule_based_interpretation(cls, operation: str, data: List[Dict[str, Any]], params: Dict[str, Any]) -> str:
        """
        Deterministic Rule-Based Business Interpretation Engine.
        Generates clear, actionable insights without LLM hallucinations.
        """
        if not data:
            return "No analytical data available for interpretation."

        if operation == "rollup":
            dim = params.get("dimension", "dimension")
            level = params.get("level", "level")
            measure = params.get("measure", "net_cost")
            top_item = max(data, key=lambda x: x.get(measure, 0))
            return f"Roll-up analysis to '{level}' level reveals '{top_item.get(params.get('level', list(top_item.keys())[0]))}' " \
                   f"has the highest {measure} at ${top_item.get(measure, 0):,.2f}."

        elif operation == "drilldown":
            next_lvl = params.get("next", "sub-level")
            measure = params.get("measure", "net_cost")
            top_item = max(data, key=lambda x: x.get(measure, 0))
            return f"Drilling down into '{next_lvl}' shows concentration in '{top_item.get(list(top_item.keys())[0])}' " \
                   f"accounting for ${top_item.get(measure, 0):,.2f} of total spend."

        return "Analytical aggregation executed successfully."

    @classmethod
    def _validate_params(cls, dimension: Optional[str] = None, measure: Optional[str] = None):
        """Security validation: Whitelist dimensions and measures to block dynamic string/SQL injection attacks."""
        if dimension and dimension.lower() not in ALLOWED_DIMENSIONS and dimension.lower() not in DIMENSION_MAPPINGS:
            raise ValueError(f"Security Rejection: Invalid or non-whitelisted dimension '{dimension}'.")
        if measure and measure.lower() not in ALLOWED_MEASURES:
            raise ValueError(f"Security Rejection: Invalid or non-whitelisted measure '{measure}'.")
