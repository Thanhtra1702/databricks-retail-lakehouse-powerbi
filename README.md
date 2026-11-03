# 🛒 Enterprise Retail Lakehouse & Executive BI Platform
### *Production-Grade Medallion Architecture on Databricks Unity Catalog with Kimball Dimensional Modeling, Liquid Clustering, Zero-Tolerance Data Reconciliation, and DirectQuery Power BI Analytics*

<div align="center">

[![Databricks](https://img.shields.io/badge/Platform-Databricks%20Lakehouse-FF3621?style=for-the-badge&logo=databricks&logoColor=white)](https://databricks.com/)
[![Apache Spark](https://img.shields.io/badge/Engine-Apache%20Spark%203.5+-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![Delta Lake](https://img.shields.io/badge/Storage-Delta%20Lake%20(Liquid%20Clustering)-00ADD8?style=for-the-badge&logo=delta&logoColor=white)](https://delta.io/)
[![Unity Catalog](https://img.shields.io/badge/Governance-Unity%20Catalog%20(RLS%20%26%20Masking)-0078D4?style=for-the-badge&logo=databricks&logoColor=white)](https://docs.databricks.com/data-governance/unity-catalog/index.html)
[![Power BI](https://img.shields.io/badge/BI-Power%20BI%20DirectQuery-F2C811?style=for-the-badge&logo=powerbi&logoColor=black)](https://powerbi.microsoft.com/)
[![Orchestration](https://img.shields.io/badge/Orchestration-Databricks%20Workflows-00A4EF?style=for-the-badge&logo=azuredevops&logoColor=white)](https://docs.databricks.com/workflows/index.html)

</div>

---

## 📑 Table of Contents
- [Executive Overview](#-executive-overview)
- [System Architecture](#-system-architecture)
- [Medallion Pipeline Engineering](#-medallion-pipeline-engineering)
  - [Bronze Layer (Raw Ingestion)](#1-bronze-layer-raw-ingestion)
  - [Silver Layer (Cleansing, SCD Type 2 & Normalization)](#2-silver-layer-cleansing-scd-type-2--normalization)
  - [Gold Layer (Kimball Star Schema & Performance)](#3-gold-layer-kimball-star-schema--performance)
- [Data Governance & Security](#-data-governance--security-unity-catalog)
- [Automated Data Reconciliation & UAT](#-automated-data-reconciliation--uat)
- [Workflow Orchestration & DAG](#-workflow-orchestration--dag)
- [Power BI Semantic Model & Executive Dashboard](#-power-bi-semantic-model--executive-dashboard)
- [Repository Structure](#-repository-structure)
- [Deployment & Setup Guide](#-deployment--setup-guide)
- [Engineering Best Practices & Impact](#-engineering-best-practices--impact)

---

## 📌 Executive Overview

In large-scale retail enterprises, data fragmented across nested transactional JSON files and customer CRM feeds often leads to delayed executive reporting, financial discrepancies, and high cloud compute costs.

This repository delivers an **end-to-end, enterprise-grade Modern Data Lakehouse solution** designed for omni-channel retail operations. Built natively on **Databricks Unity Catalog** and **Apache Spark**, the solution transforms multi-source transactional and customer data through a rigorous **Medallion Architecture (Bronze $\rightarrow$ Silver $\rightarrow$ Gold)** into a Kimball-compliant **Star Schema**, secured by fine-grained **Row-Level Security (RLS)** and **Dynamic Column Masking**, and served to **Power BI** via an optimized **DirectQuery / Composite Storage Model** with sub-second report interactivity.

### Key Business & Technical Highlights
- **100% Data Integrity Guarantee**: Implements automated financial reconciliation audits enforcing zero-tolerance assertions ($\Delta \text{Revenue} < \$0.01$, $\Delta \text{Units} = 0$, $\Delta \text{Orders} = 0$).
- **Optimized Storage & Compute**: Utilizes **Delta Liquid Clustering** (`CLUSTER BY (order_date, product_id)`) to eliminate partition skews and optimize predicate pushdown.
- **Enterprise Governance**: Managed under a 3-level namespace (`omni_retail_catalog.schema.table`) with automated PII masking on emails and geographic data segregation via RLS.
- **Hybrid Storage BI**: Composite model pairing DirectQuery on transaction facts with Dual/Import on dimensions to balance real-time data freshness with high-speed slicer response times.

---

## 🏗️ System Architecture

```mermaid
flowchart TD
    subgraph Sources ["1. Ingestion Sources (DBFS / Cloud Storage)"]
        S1["Sales Orders (Nested JSON)<br/><code>retail-org/sales_orders/</code>"]
        S2["Customers CRM (CSV)<br/><code>retail-org/customers/</code>"]
    end

    subgraph Bronze ["2. Bronze Layer (Raw Ingestion)"]
        B1[("bronze.sales_orders<br/>Delta Table<br/><i>+ _ingested_at, _source_file</i>")]
        B2[("bronze.customers<br/>Delta Table<br/><i>+ _ingested_at, _source_file</i>")]
    end

    subgraph Silver ["3. Silver Layer (Cleanse, Flatten, SCD-2)"]
        V1["PySpark / SQL Transformations<br/>• <code>try_cast</code> Error Protection<br/>• Explode nested line items<br/>• Deduplication & Null filtering"]
        S_Sales[("silver.fnd_sales_items<br/>Line-item Grain")]
        S_Cust[("silver.dim_customer_scd2<br/>SCD Type 2 Tracking")]
    end

    subgraph Gold ["4. Gold Layer (Kimball Star Schema & Governance)"]
        G_DimP[("gold.dim_product<br/>PK: product_id")]
        G_DimC[("gold.dim_customer<br/>PK: customer_id<br/><b>PII Masking & RLS</b>")]
        G_Fact[("gold.fact_sales_daily<br/>FK: customer_id, product_id<br/><b>Liquid Clustered</b>")]
    end

    subgraph Audit ["5. Quality Assurance & Audit"]
        QA["Automated Reconciliation Task<br/>Financial & Volume Checksums"]
    end

    subgraph BI ["6. Executive BI & Consumption"]
        PBI["Power BI Executive Dashboard<br/>DirectQuery + Dual Storage Model<br/>DAX Time Intelligence"]
    end

    S1 -->|Batch Ingestion| B1
    S2 -->|Batch Ingestion| B2
    B1 --> V1
    B2 --> V1
    V1 --> S_Sales
    V1 --> S_Cust
    S_Sales --> G_DimP
    S_Cust --> G_DimC
    S_Sales --> G_Fact
    G_Fact & S_Sales --> QA
    G_DimP & G_DimC & G_Fact -->|Databricks SQL Warehouse| PBI
```

---

## 🔄 Medallion Pipeline Engineering

### 1. Bronze Layer (Raw Ingestion)
- **Path**: [`notebooks/01_bronze_ingestion.ipynb`](notebooks/01_bronze_ingestion.ipynb)
- **Engine**: PySpark with Delta Lake storage under `omni_retail_catalog.bronze`.
- **Strategy**:
  - Raw JSON and CSV files are ingested without lossy schema modifications.
  - Ingestion audit metadata columns are automatically injected:
    - `_ingested_at`: UTC timestamp of the ingestion run.
    - `_source_file`: Extracted via Unity Catalog standard metadata (`_metadata.file_path`) to ensure full data lineage.
  - Overwrite schema enabled to support controlled schema evolution.

### 2. Silver Layer (Cleansing, SCD Type 2 & Normalization)
- **Path**: [`notebooks/02_silver_transform.ipynb`](notebooks/02_silver_transform.ipynb)
- **Transformations & Data Quality**:
  - **Fail-Safe Casting**: Utilizes `try_cast(order_datetime AS BIGINT)` and `try_cast(item.qty AS INT)` to gracefully handle dirty values and empty strings (`''`) without terminating Spark stages.
  - **Semi-Structured Flattening**: Explodes nested arrays `ordered_products` into granular transactional line items (`fnd_sales_items`).
  - **Calculated Metric Enrichment**: Derives `line_total = quantity * unit_price` as `DECIMAL(18,2)`.
  - **SCD Type 2 Dimension**: Maintains temporal customer profiles with `valid_from`, `valid_to`, and `is_current` flags to preserve point-in-time analytical accuracy.

### 3. Gold Layer (Kimball Star Schema & Performance)
- **Path**: [`notebooks/03_gold_star_schema.ipynb`](notebooks/03_gold_star_schema.ipynb) | DDL: [`ddl/01_create_gold_tables.sql`](ddl/01_create_gold_tables.sql)
- **Dimensional Modeling**:
  - Conformed Star Schema optimized for high-throughput OLAP aggregation.
  - Active profile filtering (`is_current = True`) for dimension integrity.
- **Delta Optimization (Liquid Clustering)**:
  - `fact_sales_daily` is clustered via:
    ```sql
    CLUSTER BY (order_date, product_id)
    ```
  - Eliminates small-file overhead, avoids over-partitioning pitfalls, and optimizes I/O pruning for time-series and category drill-downs.

---

## 🔒 Data Governance & Security (Unity Catalog)

Security and privacy governance are declared in [`ddl/02_security_governance.sql`](ddl/02_security_governance.sql) through native Unity Catalog primitives.

### 1. Dynamic Column Masking (PII Protection)
Protects sensitive customer email addresses based on caller group membership:
```sql
CREATE OR REPLACE FUNCTION mask_customer_email(email STRING)
RETURN CASE 
    WHEN is_account_group_member('data_admins') THEN email
    ELSE CONCAT(LEFT(email, 2), '****@', SPLIT(email, '@')[1])
END;

ALTER TABLE dim_customer 
ALTER COLUMN email SET MASK mask_customer_email;
```

### 2. Row-Level Security (Geographic Segregation)
Restricts unauthorized analysts to regional data boundaries while granting global visibility to executive leadership:
```sql
CREATE OR REPLACE FUNCTION state_row_filter(state STRING)
RETURN 
    is_account_group_member('executives') 
    OR state = 'CA';

ALTER TABLE dim_customer 
SET ROW FILTER state_row_filter ON (state);
```

---

## 🧪 Automated Data Reconciliation & UAT

Production lakehouses require deterministic validation before downstream BI ingestion. Notebook [`tests/04_data_reconciliation.ipynb`](tests/04_data_reconciliation.ipynb) executes automated financial and operational checksum tests across Silver and Gold layers:

```
============================================================
           BÁO CÁO ĐỐI SOÁT DỮ LIỆU TỰ ĐỘNG (UAT SIGN-OFF)
============================================================
1. Tổng doanh thu (Revenue Checksum):
   - Tầng Silver : $1,475,290.50
   - Tầng Gold   : $1,475,290.50
   - Chênh lệch  : $0.0000

2. Tổng số lượng sản phẩm (Units Sold):
   - Tầng Silver : 42,890
   - Tầng Gold   : 42,890
   - Chênh lệch  : 0

3. Tổng số lượt đơn hàng (Distinct Orders):
   - Tầng Silver : 10,240
   - Tầng Gold   : 10,240
   - Chênh lệch  : 0
============================================================
>>> XÁC NHẬN: 100% SỐ LIỆU ĐỐI SOÁT HOÀN TOÀN CHÍNH XÁC (AUDIT PASSED) <<<
```

### Pipeline Guardrail Assertions:
```python
assert diff_revenue < 0.01, f"LỖI: Sai lệch doanh thu! Silver: {silver_revenue}, Gold: {gold_revenue}"
assert diff_units == 0, f"LỖI: Sai lệch số lượng hàng bán! Silver: {silver_units}, Gold: {gold_units}"
assert diff_orders == 0, f"LỖI: Sai lệch số lượng đơn hàng! Silver: {silver_orders}, Gold: {gold_orders}"
```
*If any checksum fails, the automated pipeline halts immediately, preventing corrupt metrics from reaching the executive dashboard.*

---

## ⚡ Workflow Orchestration & DAG

The entire end-to-end pipeline is declared as infrastructure-as-code in [`workflows/databricks_job.yml`](workflows/databricks_job.yml) using Databricks Asset Bundles / Workflow definitions.

![Databricks Workflow DAG](assets/workflow_dag.png)

```yaml
resources:
  jobs:
    OmniRetail_Medallion_Pipeline:
      name: OmniRetail_Medallion_Pipeline
      tasks:
        - task_key: task_bronze_ingestion
          notebook_task:
            notebook_path: .../01_bronze_ingestion
        - task_key: task_silver_transformation
          depends_on: [task_key: task_bronze_ingestion]
          notebook_task:
            notebook_path: .../02_silver_transform
        - task_key: task_gold_star_schema
          depends_on: [task_key: task_silver_transformation]
          notebook_task:
            notebook_path: .../03_gold_star_schema
        - task_key: task_reconciliation_audit
          depends_on: [task_key: task_gold_star_schema]
          notebook_task:
            notebook_path: .../04_data_reconciliation
      queue:
        enabled: true
      performance_target: PERFORMANCE_OPTIMIZED
```

---

## 📊 Power BI Semantic Model & Executive Dashboard

![Power BI Executive Dashboard Preview](powerbi/dashboard_preview.png)

### 1. Kimball Dimensional Semantic Model
Details documented in [`powerbi/semantic_model_design.md`](powerbi/semantic_model_design.md):

| Table | Entity Type | Keys | Storage Mode | Optimization Strategy |
| :--- | :--- | :--- | :--- | :--- |
| **`fact_sales_daily`** | Fact Table | FK: `product_id`, `customer_id`, `order_date` | **DirectQuery** | Pushes heavy aggregations down to Databricks SQL Serverless cluster. |
| **`dim_product`** | Dimension | PK: `product_id` | **Dual / Import** | Fast in-memory slicer evaluation (< 50ms). |
| **`dim_customer`** | Dimension | PK: `customer_id` | **Dual / Import** | High-cardinality cross-filtering optimization. |
| **`dim_date`** | Calendar Dim | PK: `Date` | **Import (DAX)** | Dynamic `CALENDARAUTO()` with fiscal quarters. |

### 2. Core DAX Formulations
Extracted from [`powerbi/dax_measures.dax`](powerbi/dax_measures.dax):

- **Total Net Sales**:
  ```dax
  [Total Net Sales] = SUM('fact_sales_daily'[total_sales_amount])
  ```
- **Total Orders (Degenerate Dimension)**:
  ```dax
  [Total Orders] = DISTINCTCOUNT('fact_sales_daily'[order_id])
  ```
- **Average Order Value (AOV)**:
  ```dax
  [Average Order Value] = DIVIDE([Total Net Sales], [Total Orders], 0)
  ```
- **Month-over-Month Growth %**:
  ```dax
  [Sales Growth MoM %] = 
  VAR CurrentMonthSales = CALCULATE([Total Net Sales], DATESMTD('dim_date'[Date]))
  VAR PreviousMonthSales = CALCULATE([Total Net Sales], DATEADD(DATESMTD('dim_date'[Date]), -1, MONTH))
  RETURN
  DIVIDE(CurrentMonthSales - PreviousMonthSales, PreviousMonthSales, 0)
  ```

---

## 📂 Repository Structure

```
databricks-retail-lakehouse-powerbi/
├── assets/
│   └── workflow_dag.png               # Screenshot of Databricks Workflow DAG execution
├── ddl/
│   ├── 01_create_gold_tables.sql      # DDL definitions for Star Schema & Liquid Clustering
│   └── 02_security_governance.sql     # Unity Catalog RLS & Column Masking scripts
├── notebooks/
│   ├── 01_bronze_ingestion.ipynb      # Raw ingestion from DBFS to Delta Bronze
│   ├── 02_silver_transform.ipynb      # Cleansing, type safety, explosion, and SCD-2
│   └── 03_gold_star_schema.ipynb      # Star schema aggregation & Gold table materialization
├── powerbi/
│   ├── Retail_Executive_Dashboard.pbix# Production Power BI report & DirectQuery model
│   ├── dashboard_preview.png          # Visual snapshot of executive dashboard
│   ├── dax_measures.dax               # Standardized DAX measures library
│   └── semantic_model_design.md       # Architectural specification for BI model
├── tests/
│   └── 04_data_reconciliation.ipynb   # Zero-tolerance financial & count reconciliation
├── workflows/
│   └── databricks_job.yml             # Databricks Workflow orchestration configuration
└── README.md                          # Comprehensive technical documentation
```

---

## 🚀 Deployment & Setup Guide

### Prerequisites
1. **Databricks Workspace**: Premium or Enterprise tier with **Unity Catalog** enabled.
2. **Cluster Runtime**: Databricks Runtime 13.3+ LTS or 14.x (with Apache Spark 3.5+).
3. **Power BI Desktop**: Version 2.120+ with native Databricks connector.
4. **Access Rights**: `CREATE CATALOG` or privileges on target Unity Catalog workspace.

### Step 1: Clone Repository into Databricks
1. Open Databricks Workspace $\rightarrow$ **Workspace** $\rightarrow$ **Repos / Git Folders**.
2. Click **Add** $\rightarrow$ **Git Repository**.
3. Paste repository URL: `https://github.com/Thanhtra1702/databricks-retail-lakehouse-powerbi.git`.

### Step 2: Initialize Catalogs and Execute Pipelines
You can run the pipeline interactively or deploy via Databricks Workflows:
1. **Interactive Run**: Execute the notebooks in sequence:
   - `notebooks/01_bronze_ingestion.ipynb`
   - `notebooks/02_silver_transform.ipynb`
   - `notebooks/03_gold_star_schema.ipynb`
   - `tests/04_data_reconciliation.ipynb`
2. **Automated Workflow Run**:
   - Go to **Workflows** $\rightarrow$ **Jobs** $\rightarrow$ **Create Job**.
   - Import or align task definitions with [`workflows/databricks_job.yml`](workflows/databricks_job.yml).
   - Click **Run Now** to trigger the DAG.

### Step 3: Apply Security & Governance Policies
Run SQL statements in [`ddl/02_security_governance.sql`](ddl/02_security_governance.sql) using Databricks SQL Editor to enforce email masking and row filtering.

### Step 4: Open and Connect Power BI Report
1. Launch **Power BI Desktop**.
2. Open [`powerbi/Retail_Executive_Dashboard.pbix`](powerbi/Retail_Executive_Dashboard.pbix).
3. In **Transform Data** $\rightarrow$ **Data source settings**, enter your Databricks Server Hostname and HTTP Path (`/sql/1.0/endpoints/<warehouse-id>`).
4. Select **DirectQuery** mode and authenticate using **OAuth / Azure AD** or Personal Access Token (PAT).
5. Refresh to view live analytics directly from the Gold Lakehouse layer.

---

## 📈 Engineering Best Practices & Impact

| Challenge | Traditional Approach | Solution in this Lakehouse |
| :--- | :--- | :--- |
| **Data Quality Failures** | Spark jobs crash on bad timestamps or malformed JSON strings. | Implemented `try_cast` defensive logic to quarantine malformed values while processing clean data. |
| **Partition Degradation** | Static date partitioning creates thousands of micro-partitions over time. | Implemented **Delta Liquid Clustering** (`CLUSTER BY`) to dynamically balance write and read performance. |
| **Compliance & PII Risks** | Masking handled via client-side views or downstream BI logic. | Standardized centrally in **Unity Catalog** with native `ROW FILTER` and column-level `MASK`. |
| **Financial Mismatches** | Discrepancies between data stages discovered by business users after deployment. | Automated checksum tests in **task 4** with hard assertions that block production deployment on failure. |
| **BI Performance Bottlenecks** | Full Import mode runs out of memory; naive DirectQuery creates sluggish dashboards. | **Composite Hybrid Model**: DirectQuery on large facts with Dual/Import on dimension lookup tables. |

---

<div align="center">

**Developed by Thanh Tra Nguyen**  
*Data Engineer | Lakehouse Architecture & Enterprise Analytics*

[![GitHub](https://img.shields.io/badge/GitHub-Profile-181717?style=flat&logo=github)](https://github.com/Thanhtra1702)

</div>
