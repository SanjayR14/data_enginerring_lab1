# Phase 5 Data Warehouse: Snowflake Schema Documentation

## 1. Overview & Normalization Strategy

While the **Star Schema** maintains denormalized dimensions for maximum query speed and minimal joins, the **Snowflake Schema** normalizes dimension tables into hierarchical sub-dimensions.

This reduces data redundancy, enforces strict structural normalization, and simplifies hierarchical updates across parent categories.

---

## 2. Snowflake Schema Diagram (Mermaid)

```mermaid
erDiagram
    fact_cloud_cost {
        int fact_id PK
        int date_key FK
        int project_key FK
        int cost_center_key FK
        int location_key FK
        int service_key FK
        int environment_key FK
        int currency_key FK
        float net_cost
        float list_cost
        float total_savings
    }

    dim_project {
        int project_key PK
        string project_id
        int account_key FK
        string environment
        boolean is_current
    }

    dim_account {
        int account_key PK
        string account_id
        int cloud_key FK
        string account_name
    }

    dim_cloud {
        int cloud_key PK
        string cloud_provider
        string provider_group
    }

    dim_cost_center {
        int cost_center_key PK
        string cost_center
        int department_key FK
    }

    dim_department {
        int department_key PK
        string department
        int business_unit_key FK
    }

    dim_business_unit {
        int business_unit_key PK
        string business_unit
    }

    fact_cloud_cost }|--|| dim_project : "project_key"
    dim_project }|--|| dim_account : "account_key"
    dim_account }|--|| dim_cloud : "cloud_key"

    fact_cloud_cost }|--|| dim_cost_center : "cost_center_key"
    dim_cost_center }|--|| dim_department : "department_key"
    dim_department }|--|| dim_business_unit : "business_unit_key"
```

---

## 3. Normalized Hierarchies

### Hierarchy 1: Organization Structure
- **Level 1**: `dim_business_unit` (`business_unit_key` PK, `business_unit`)
- **Level 2**: `dim_department` (`department_key` PK, `business_unit_key` FK, `department`)
- **Level 3**: `dim_cost_center` (`cost_center_key` PK, `department_key` FK, `cost_center`)

### Hierarchy 2: Cloud Infrastructure Hierarchy
- **Level 1**: `dim_cloud` (`cloud_key` PK, `cloud_provider`, `provider_group`)
- **Level 2**: `dim_account` (`account_key` PK, `cloud_key` FK, `account_id`)
- **Level 3**: `dim_project` (`project_key` PK, `account_key` FK, `project_id`)

---

## 4. Star Schema vs Snowflake Schema Comparison

| Dimension | Star Schema | Snowflake Schema |
|---|---|---|
| **Structure** | Single denormalized table per dimension (e.g. `dim_organization` contains BU, Dept, Cost Center) | Multi-level normalized sub-dimensions (`dim_business_unit` -> `dim_department` -> `dim_cost_center`) |
| **Joins Required** | 1 join per dimension from fact table | Multi-level chained joins (`fact` -> `cost_center` -> `department` -> `business_unit`) |
| **Storage & Redundancy** | Higher attribute redundancy inside dimensions | Maximum normalization, zero attribute redundancy |
| **Query Performance** | Faster for aggregations and dashboard queries | Slightly slower due to multi-table join depth |
| **Maintenance** | Single table updates | Clean cascade updates across sub-dimension tables |
