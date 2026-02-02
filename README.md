# ERP Lakehouse Data Engineering Demo

This project demonstrates an **enterprise-grade Lakehouse data architecture** inspired by ERP systems such as **Navision / Business Central**.

The objective is to showcase **data engineering best practices** including:
- Layered Lakehouse design (Bronze / Silver / Gold)
- Delta Lake tables
- Star schema modeling
- Basic data quality checks
- Analytics-ready datasets

 **Important**  
All data used in this project is **synthetic and artificially generated**.  
No proprietary company systems, schemas, or data are used.

---

## 1. Architecture Overview

The project follows a modern Lakehouse architecture:

Synthetic ERP CSV Data  
→ Landing Zone (Databricks Volume)  
→ Bronze Layer (Raw Delta Tables)  
→ Silver Layer (Cleaned & Conformed)  
→ Gold Layer (Star Schema for Analytics)

- **Bronze**: raw ingested data with enforced schema  
- **Silver**: cleaned, deduplicated, and join-ready data  
- **Gold**: analytics-ready fact and dimension tables  

---

## 2. Dataset Scope (15 ERP-like Tables)

The dataset represents a realistic subset of an ERP system.

### Master Data
- Customer  
- Vendor  
- Item  
- Item Category  
- Location  
- Unit of Measure  
- Currency  

### Sales & Inventory
- Sales Header  
- Sales Line  
- Item Ledger Entry  
- Value Entry  

### Finance / Ledger
- G/L Account  
- G/L Entry  
- Customer Ledger Entry  
- Vendor Ledger Entry  

The scope is intentionally limited to focus on **design quality and scalability**, not completeness.

---

## 3. Technology Stack

- Databricks (Free Edition)  
- Delta Lake  
- Apache Spark (SQL & PySpark)  
- Unity Catalog concepts (Catalog / Schema / Volume)  
- GitHub  

---

## 4. Catalog & Data Organization

Catalog: `erp_demo`

Schemas:
- `bronze` – raw ingested data  
- `silver` – cleaned & conformed data  
- `gold` – analytics-ready data  

Landing data location:

/Volumes/erp_demo/bronze/landing/

This simulates an enterprise cloud data lake landing zone.

---

## 5. Bronze Layer (Raw Ingestion)

- CSV files loaded from landing volume  
- Explicit schemas applied at read time  
- Data written as Delta tables  

Examples:
- erp_demo.bronze.bronze_customer  
- erp_demo.bronze.bronze_sales_header  

Purpose:
- Preserve raw data  
- Ensure schema consistency  
- Enable reproducibility  

---

## 6. Silver Layer (Clean & Conform)

Silver transformations include:
- Type validation  
- Deduplication  
- Key integrity checks  
- Standardized naming  
- Load timestamp tracking  

Examples:
- erp_demo.silver.customer  
- erp_demo.silver.sales_header  
- erp_demo.silver.sales_line  

Silver tables are join-ready and suitable for business logic.

---

## 7. Gold Layer (Star Schema)

### Dimension Tables
- dim_customer  
- dim_item  
- dim_location  
- dim_date  

### Fact Tables
- fact_sales (order-line grain)  
- fact_inventory_movement  
- fact_gl  

These tables follow a **star schema design**, optimized for BI and SQL analytics.

---

## 8. Date Dimension

A conformed `dim_date` is automatically generated based on the date ranges found in fact tables.

Attributes include:
- Year  
- Quarter  
- Month  
- Week of year  
- Day of week  
- Weekend flag  

---

## 9. Data Quality Checks

Basic data quality checks are implemented:
- Not-null validation on key fields  
- Referential integrity between facts and dimensions  

In production, these checks would be automated and monitored.

---

## 10. How to Run

1. Upload CSV files to:

/Volumes/erp_demo/bronze/landing/

2. Run notebooks in order:
- 01_load_bronze  
- 02_build_silver  
- 03_build_gold  

3. Validate Gold tables using SQL or Databricks UI.

---

## 11. Intended Audience

This project is designed for:
- Data Engineers  
- Analytics Engineers  
- BI / Analytics Architects  
- Technical Managers  

The focus is on **engineering design and data modeling**, not tool-specific shortcuts.

---

## 12. Disclaimer

This project is for educational and demonstration purposes only.  
All data is synthetic. Any resemblance to real systems is purely structural.

