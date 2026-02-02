# Databricks notebook source
spark.sql("USE CATALOG erp_demo")
spark.sql("USE SCHEMA silver")
display(spark.sql("SELECT current_catalog() AS catalog, current_schema() AS schema"))

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# ---- Customer ----
cust = (
    spark.table("erp_demo.bronze.bronze_customer")
    .dropDuplicates(["customer_id"])
    .withColumn("load_time", current_timestamp())
)
cust.write.format("delta").mode("overwrite").saveAsTable("erp_demo.silver.customer")

# ---- Vendor ----
vend = (
    spark.table("erp_demo.bronze.bronze_vendor")
    .dropDuplicates(["vendor_id"])
    .withColumn("load_time", current_timestamp())
)
vend.write.format("delta").mode("overwrite").saveAsTable("erp_demo.silver.vendor")

# ---- Item ----
it = (
    spark.table("erp_demo.bronze.bronze_item")
    .dropDuplicates(["item_id"])
    .withColumn("load_time", current_timestamp())
)
it.write.format("delta").mode("overwrite").saveAsTable("erp_demo.silver.item")

# ---- Item Category ----
icat = (
    spark.table("erp_demo.bronze.bronze_item_category")
    .dropDuplicates(["item_category"])
    .withColumn("load_time", current_timestamp())
)
icat.write.format("delta").mode("overwrite").saveAsTable("erp_demo.silver.item_category")

# ---- Location ----
loc = (
    spark.table("erp_demo.bronze.bronze_location")
    .dropDuplicates(["location_code"])
    .withColumn("load_time", current_timestamp())
)
loc.write.format("delta").mode("overwrite").saveAsTable("erp_demo.silver.location")

# ---- UOM ----
uom = (
    spark.table("erp_demo.bronze.bronze_unit_of_measure")
    .dropDuplicates(["uom_code"])
    .withColumn("load_time", current_timestamp())
)
uom.write.format("delta").mode("overwrite").saveAsTable("erp_demo.silver.unit_of_measure")

# ---- Currency ----
cur = (
    spark.table("erp_demo.bronze.bronze_currency")
    .dropDuplicates(["currency_code"])
    .withColumn("load_time", current_timestamp())
)
cur.write.format("delta").mode("overwrite").saveAsTable("erp_demo.silver.currency")


# COMMAND ----------

display(spark.table("erp_demo.silver.customer").limit(5))


# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp

# ---- Sales Header ----
sh = (
    spark.table("erp_demo.bronze.bronze_sales_header")
    .filter(col("sales_order_id").isNotNull())
    .dropDuplicates(["sales_order_id"])
    .withColumn("load_time", current_timestamp())
)
sh.write.format("delta").mode("overwrite").saveAsTable("erp_demo.silver.sales_header")

# ---- Sales Line ----
sl = (
    spark.table("erp_demo.bronze.bronze_sales_line")
    .filter(col("sales_order_id").isNotNull())
    .filter(col("line_no").isNotNull())
    .withColumn("load_time", current_timestamp())
)
sl.write.format("delta").mode("overwrite").saveAsTable("erp_demo.silver.sales_line")

# ---- Item Ledger Entry ----
ile = (
    spark.table("erp_demo.bronze.bronze_item_ledger_entry")
    .filter(col("entry_id").isNotNull())
    .withColumn("load_time", current_timestamp())
)
ile.write.format("delta").mode("overwrite").saveAsTable("erp_demo.silver.item_ledger_entry")

# ---- Value Entry ----
ve = (
    spark.table("erp_demo.bronze.bronze_value_entry")
    .filter(col("entry_id").isNotNull())
    .withColumn("load_time", current_timestamp())
)
ve.write.format("delta").mode("overwrite").saveAsTable("erp_demo.silver.value_entry")

# ---- G/L Account ----
gla = (
    spark.table("erp_demo.bronze.bronze_gl_account")
    .dropDuplicates(["gl_account_no"])
    .withColumn("load_time", current_timestamp())
)
gla.write.format("delta").mode("overwrite").saveAsTable("erp_demo.silver.gl_account")

# ---- G/L Entry ----
gle = (
    spark.table("erp_demo.bronze.bronze_gl_entry")
    .filter(col("entry_id").isNotNull())
    .withColumn("load_time", current_timestamp())
)
gle.write.format("delta").mode("overwrite").saveAsTable("erp_demo.silver.gl_entry")

# ---- Cust Ledger Entry ----
cle = (
    spark.table("erp_demo.bronze.bronze_customer_ledger_entry")
    .filter(col("entry_id").isNotNull())
    .withColumn("load_time", current_timestamp())
)
cle.write.format("delta").mode("overwrite").saveAsTable("erp_demo.silver.customer_ledger_entry")

# ---- Vendor Ledger Entry ----
vle = (
    spark.table("erp_demo.bronze.bronze_vendor_ledger_entry")
    .filter(col("entry_id").isNotNull())
    .withColumn("load_time", current_timestamp())
)
vle.write.format("delta").mode("overwrite").saveAsTable("erp_demo.silver.vendor_ledger_entry")



# COMMAND ----------

q = """
select
  sh.sales_order_id,
  sh.order_date,
  sh.status,
  c.customer_name,
  sl.line_no,
  i.item_name,
  sl.quantity,
  sl.unit_price,
  sl.quantity * sl.unit_price as line_amount
from erp_demo.silver.sales_header sh
join erp_demo.silver.customer c
  on sh.customer_id = c.customer_id
join erp_demo.silver.sales_line sl
  on sh.sales_order_id = sl.sales_order_id
join erp_demo.silver.item i
  on sl.item_id = i.item_id
limit 20
"""
display(spark.sql(q))
