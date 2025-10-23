# 🍕 HB Pizza Analytics Project

## Overview

This project simulates a full-stack data analytics pipeline for a fictitious pizza and wings business, Hunt Advantage Group LLC. It showcases real-world data engineering practices using a medallion architecture (Bronze → Silver → Gold), Power BI reporting, and strategic orchestration under platform and licensing constraints. This project injects real world data chaos with the use of autogen python scripts causing correlations but not causations. It is an ongoing experiment to hone in on data wrangling skills.

The goal is to demonstrate technical depth, business storytelling, and adaptability in building scalable analytics solutions.

Current phase:
Migrating silver layers that were in Microsoft Fabric into Metabase.

---

## Architecture

### 🥉 Bronze Layer – Raw Ingestion
- **Source**: Simulated CSV datasets with randomized rows and injected data quality issues
- **Files**:
  - `orders.csv`
  - `feedback.csv`
  - `inventory.csv`
  - `locations.csv`
  - `store_metrics.csv`

### 🥈 Silver Layer – Cleaned & Enriched
- **Tools**: Python (Pandas, TextBlob), Power Query (via Dataflow Gen1)
- **Transformations**:
  - Normalized column names and types
  - Cleaned nulls, fixed casing, trimmed strings
  - Calculated fields (e.g., `pizza_combo`, sentiment polarity)
  - Joined related datasets

### 🥇 Gold Layer – Modeled & Aggregated
- **Star Schema**:
  - `fact_orders`
  - `dim_store`
  - `dim_product`
  - `dim_date`
  - `fact_feedback`
  - `fact_metrics`
- **DAX Measures**:
  - `Total Sales`
  - `Average Order Value`
  - `Wings Mix`
  - `Avg Sentiment`

---

## Reporting

- **Tool**: Power BI Desktop
- **Report**: `pizza_reporting.pbix`
- **Version Control**: `.pbit` templates stored in `/powerbi_templates/`
- **Visuals**:
  - Stacked columns for categorical comparisons.
  - Treemaps for Sales
  - Stacked bar for toppings count
  - Cluster column for date comparisons
  - Stacked columns for trend averages

---

## Platform Constraints & Workarounds

| Restriction | Workaround |
|------------|------------|
| Fabric trial provisioning expired | Pivoted to local medallion architecture using CSVs and Python |
| No OneDrive for Business | Used Power BI Desktop with local files and manual upload |
| Limited Dataflow Gen1 connectors | Published `.pbix` to activate workspace and unlock gateway |
| No Power BI Pro license | Used “Upload” feature in Power BI Service |
| Large `.pbix` files not Git-friendly | Used `.pbit` templates and changelog discipline |

---

## Sharing & Access

- **Fabric Workspace**: Bishappcore (access pending domain trust configuration)
- **Embedded Report**: Planned post-Gold layer finalization
- **Note**: This project is fictitious and intended for demonstration purposes only

---

## Repositories

- [GitHub – HB Pizza Analytics](https://github.com/jkbishop8/hbpizza)

---

## Author

**Jeremy Bishop**  
Aspiring Data Engineer / Analyst  
Focused on building impactful, interview-ready analytics solutions under real-world constraints.

