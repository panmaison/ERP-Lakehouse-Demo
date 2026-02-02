# Databricks notebook source
spark.sql("USE CATALOG erp_demo")
spark.sql("USE SCHEMA gold")
display(spark.sql("SELECT current_catalog() AS catalog, current_schema() AS schema"))

# COMMAND ----------

# ---- dim_customer ----
spark.sql("""
CREATE OR REPLACE TABLE erp_demo.gold.dim_customer AS
SELECT
  customer_id,
  customer_name,
  country,
  created_at
FROM erp_demo.silver.customer
""")

# ---- dim_item ----
spark.sql("""
CREATE OR REPLACE TABLE erp_demo.gold.dim_item AS
SELECT
  item_id,
  item_name,
  item_category,
  unit_cost
FROM erp_demo.silver.item
""")

# ---- dim_location ----
spark.sql("""
CREATE OR REPLACE TABLE erp_demo.gold.dim_location AS
SELECT
  location_code,
  location_name
FROM erp_demo.silver.location
""")

display(spark.table("erp_demo.gold.dim_customer").limit(5))
display(spark.table("erp_demo.gold.dim_item").limit(5))
display(spark.table("erp_demo.gold.dim_location").limit(5))


# COMMAND ----------

spark.sql("""
CREATE OR REPLACE TABLE erp_demo.gold.fact_sales AS
SELECT
  sh.sales_order_id,
  sh.order_date,
  sh.status,
  sh.customer_id,
  sl.line_no,
  sl.item_id,
  sl.quantity,
  sl.unit_price,
  sl.quantity * sl.unit_price AS line_amount
FROM erp_demo.silver.sales_header sh
JOIN erp_demo.silver.sales_line sl
  ON sh.sales_order_id = sl.sales_order_id
""")

display(spark.table("erp_demo.gold.fact_sales").limit(10))

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE TABLE erp_demo.gold.fact_inventory_movement AS
SELECT
  entry_id,
  posting_date,
  item_id,
  location_code,
  quantity
FROM erp_demo.silver.item_ledger_entry
""")

display(spark.table("erp_demo.gold.fact_inventory_movement").limit(10))


# COMMAND ----------

spark.sql("""
CREATE OR REPLACE TABLE erp_demo.gold.fact_gl AS
SELECT
  e.entry_id,
  e.posting_date,
  e.gl_account_no,
  a.gl_account_name,
  a.account_type,
  e.amount
FROM erp_demo.silver.gl_entry e
LEFT JOIN erp_demo.silver.gl_account a
  ON e.gl_account_no = a.gl_account_no
""")

display(spark.table("erp_demo.gold.fact_gl").limit(10))


# COMMAND ----------

dq = {}

# 1) not null checks
dq["fact_sales_null_sales_order_id"] = spark.sql("""
SELECT COUNT(*) AS cnt
FROM erp_demo.gold.fact_sales
WHERE sales_order_id IS NULL
""").collect()[0]["cnt"]

dq["fact_sales_null_item_id"] = spark.sql("""
SELECT COUNT(*) AS cnt
FROM erp_demo.gold.fact_sales
WHERE item_id IS NULL
""").collect()[0]["cnt"]

# 2) referential integrity checks
dq["fact_sales_missing_customer"] = spark.sql("""
SELECT COUNT(*) AS cnt
FROM erp_demo.gold.fact_sales f
LEFT JOIN erp_demo.gold.dim_customer c
  ON f.customer_id = c.customer_id
WHERE c.customer_id IS NULL
""").collect()[0]["cnt"]

dq["fact_sales_missing_item"] = spark.sql("""
SELECT COUNT(*) AS cnt
FROM erp_demo.gold.fact_sales f
LEFT JOIN erp_demo.gold.dim_item i
  ON f.item_id = i.item_id
WHERE i.item_id IS NULL
""").collect()[0]["cnt"]

dq


# COMMAND ----------

date_range = spark.sql("""
SELECT
  MIN(d) AS min_date,
  MAX(d) AS max_date
FROM (
  SELECT order_date AS d FROM erp_demo.gold.fact_sales
  UNION ALL
  SELECT posting_date AS d FROM erp_demo.gold.fact_gl
  UNION ALL
  SELECT posting_date AS d FROM erp_demo.gold.fact_inventory_movement
)
""").collect()[0]

min_date = date_range["min_date"]
max_date = date_range["max_date"]

min_date, max_date


# COMMAND ----------

from pyspark.sql.functions import (
    col, year, month, dayofmonth, weekofyear,
    quarter, date_format, expr
)

dates_df = (
    spark.sql(f"""
    SELECT explode(
        sequence(
            date('{min_date}'),
            date('{max_date}'),
            interval 1 day
        )
    ) AS date
    """)
)


# COMMAND ----------

dim_date = (
    dates_df
    .withColumn("date_key", date_format(col("date"), "yyyyMMdd").cast("int"))
    .withColumn("year", year(col("date")))
    .withColumn("quarter", quarter(col("date")))
    .withColumn("month", month(col("date")))
    .withColumn("month_name", date_format(col("date"), "MMMM"))
    .withColumn("day", dayofmonth(col("date")))
    .withColumn("week_of_year", weekofyear(col("date")))
    .withColumn("day_of_week", date_format(col("date"), "E"))
    .withColumn("is_weekend", expr("dayofweek(date) IN (1,7)"))
)

dim_date.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("erp_demo.gold.dim_date")

display(spark.table("erp_demo.gold.dim_date").limit(10))
