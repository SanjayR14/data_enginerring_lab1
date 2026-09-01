# Phase 6: Analytical Queries & Business Interpretation Guide

This document details the 12 primary OLAP analytical query patterns implemented in the `OLAPEngine` service and FastAPI REST endpoints (`/api/olap/*`).

---

### 1. Roll-up Query
- **Purpose**: Moves up the analytical hierarchy from granular levels to higher summary aggregations (e.g., Daily $\rightarrow$ Monthly $\rightarrow$ Yearly).
- **Dimensions**: `Time` (`month_name`, `year`), `Organization` (`business_unit`).
- **Measures**: `net_cost`, `total_savings`.
- **API Endpoint**: `GET /api/olap/rollup?dimension=time&level=month&measure=net_cost`
- **Expected Interpretation**: Identifies macro-level cost distribution across months or higher-level business units.

---

### 2. Drill-down Query
- **Purpose**: Decomposes summary totals into lower granular components (e.g., Year $2023 \rightarrow$ Quarter $Q1 \rightarrow$ January $\rightarrow$ Daily).
- **Dimensions**: `Time` (`year` $\rightarrow$ `quarter` $\rightarrow$ `month`), `Cloud` (`cloud_provider` $\rightarrow$ `account_id` $\rightarrow$ `project_id`).
- **Measures**: `net_cost`, `budget_amount`.
- **API Endpoint**: `GET /api/olap/drilldown?hierarchy=time&current_level=year&next_level=quarter`
- **Expected Interpretation**: Isolates specific sub-periods or projects driving cost increases.

---

### 3. Slice Query
- **Purpose**: Fixes a single dimension to a static value (e.g., `Cloud Provider = AWS`).
- **Dimensions**: Fixed dimension: `cloud_provider` ('AWS'); Free dimensions: `department`, `service`.
- **Measures**: `net_cost`.
- **API Endpoint**: `GET /api/olap/slice?dimension=cloud_provider&value=AWS&measure=net_cost`
- **Expected Interpretation**: Evaluates expenditure isolated strictly to a single cloud provider environment.

---

### 4. Dice Query
- **Purpose**: Applies multi-dimensional filtering constraints simultaneously to create a filtered sub-cube.
- **Dimensions**: `cloud_provider` = `['AWS', 'Azure']`, `department` = `['Engineering', 'Finance']`, `environment` = `'production'`.
- **Measures**: `net_cost`, `budget_amount`, `total_savings`.
- **API Endpoint**: `POST /api/olap/dice`
- **Expected Interpretation**: Evaluates production cloud spend for selected cross-functional teams.

---

### 5. Pivot Query
- **Purpose**: Re-orients axes to create a 2D cross-tabulation matrix (Rows $\times$ Columns).
- **Dimensions**: Rows: `department`; Columns: `cloud_provider`.
- **Measures**: `net_cost`.
- **API Endpoint**: `GET /api/olap/pivot?rows=department&columns=cloud_provider&measure=net_cost`
- **Expected Interpretation**: Provides an immediate heatmap matrix comparing departmental spending across cloud vendors.

---

### 6. Top-N Analysis Query
- **Purpose**: Ranks top spending entities and calculates percentage contributions to overall cloud cost.
- **Dimensions**: `project_id`, `service`, `department`, `region`.
- **Measures**: `net_cost`.
- **API Endpoint**: `GET /api/olap/top?category=projects&n=10`
- **Expected Interpretation**: Highlights Pareto 80/20 cost drivers across projects and services.

---

### 7. Time-Series Analysis Query
- **Purpose**: Generates chronological cost sequences for trend monitoring and forecasting.
- **Dimensions**: `date` / `year` / `month` (ordered chronologically).
- **Measures**: `net_cost`, `total_savings`.
- **API Endpoint**: `GET /api/olap/time-series?granularity=monthly`
- **Expected Interpretation**: Tracks trajectory of monthly cloud expenditure and seasonality.

---

### 8. Budget Analysis Query
- **Purpose**: Evaluates actual expenditure against target budgets and classifies departments into `Under Budget`, `Near Budget`, or `Over Budget`.
- **Dimensions**: `department`, `business_unit`.
- **Measures**: `budget_amount`, `net_cost`, `budget_remaining`, `budget_utilization_pct`.
- **API Endpoint**: `GET /api/olap/budget`
- **Expected Interpretation**: Flags financial risk areas approaching or exceeding budget limits.

---

### 9. Savings Analysis Query
- **Purpose**: Quantifies cost optimizations achieved through commitment models and discounts.
- **Dimensions**: `cloud_provider`, `department`, `service`.
- **Measures**: `reserved_savings`, `savings_plan_savings`, `spot_savings`, `total_savings`, `effective_discount_pct`.
- **API Endpoint**: `GET /api/olap/savings`
- **Expected Interpretation**: Demonstrates ROI of commitment purchases and discount programs.

---

### 10. Anomaly Analysis Query
- **Purpose**: Identifies high-risk spike events and anomaly rates.
- **Dimensions**: `service`, `department`, `region`.
- **Measures**: `anomaly_score`, `is_anomaly`, `net_cost`.
- **API Endpoint**: `GET /api/olap/anomalies`
- **Expected Interpretation**: Pinpoints unexpected cost spikes requiring engineering remediation.

---

### 11. Provider Comparison Query
- **Purpose**: Side-by-side comparative analysis of multi-cloud vendor environments.
- **Dimensions**: `cloud_provider` (`AWS` vs `Azure` vs `GCP`).
- **Measures**: `net_cost`, `budget_amount`, `total_savings`.
- **API Endpoint**: `GET /api/olap/compare?dimension=cloud_provider`
- **Expected Interpretation**: Benchmarks multi-cloud unit economics and efficiency.

---

### 12. Department Comparison Query
- **Purpose**: Comparative breakdown across organizational business units and departments.
- **Dimensions**: `department`.
- **Measures**: `net_cost`, `budget_utilization_pct`, `total_savings`.
- **API Endpoint**: `GET /api/olap/compare?dimension=department`
- **Expected Interpretation**: Compares cost efficiency and budget discipline across organizational teams.
