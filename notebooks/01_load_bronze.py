# Databricks notebook source
spark.sql("USE CATALOG erp_demo")
spark.sql("USE SCHEMA bronze")
display(spark.sql("SELECT current_catalog() AS catalog, current_schema() AS schema"))

# COMMAND ----------

from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType, DateType
)

schemas = {
  "customer": StructType([
      StructField("customer_id", IntegerType(), True),
      StructField("customer_name", StringType(), True),
      StructField("country", StringType(), True),
      StructField("created_at", DateType(), True),
  ]),
  "vendor": StructType([
      StructField("vendor_id", IntegerType(), True),
      StructField("vendor_name", StringType(), True),
      StructField("country", StringType(), True),
      StructField("created_at", DateType(), True),
  ]),
  "item": StructType([
      StructField("item_id", IntegerType(), True),
      StructField("item_name", StringType(), True),
      StructField("item_category", StringType(), True),
      StructField("unit_cost", DoubleType(), True),
  ]),
  "item_category": StructType([
      StructField("item_category", StringType(), True),
      StructField("description", StringType(), True),
  ]),
  "location": StructType([
      StructField("location_code", StringType(), True),
      StructField("location_name", StringType(), True),
  ]),
  "unit_of_measure": StructType([
      StructField("uom_code", StringType(), True),
      StructField("description", StringType(), True),
  ]),
  "currency": StructType([
      StructField("currency_code", StringType(), True),
      StructField("description", StringType(), True),
  ]),
  "gl_account": StructType([
      StructField("gl_account_no", IntegerType(), True),
      StructField("gl_account_name", StringType(), True),
      StructField("account_type", StringType(), True),
  ]),
  "gl_entry": StructType([
      StructField("entry_id", IntegerType(), True),
      StructField("gl_account_no", IntegerType(), True),
      StructField("amount", DoubleType(), True),
      StructField("posting_date", DateType(), True),
  ]),
  "customer_ledger_entry": StructType([
      StructField("entry_id", IntegerType(), True),
      StructField("customer_id", IntegerType(), True),
      StructField("amount", DoubleType(), True),
      StructField("posting_date", DateType(), True),
  ]),
  "vendor_ledger_entry": StructType([
      StructField("entry_id", IntegerType(), True),
      StructField("vendor_id", IntegerType(), True),
      StructField("amount", DoubleType(), True),
      StructField("posting_date", DateType(), True),
  ]),
  "sales_header": StructType([
      StructField("sales_order_id", IntegerType(), True),
      StructField("customer_id", IntegerType(), True),
      StructField("order_date", DateType(), True),
      StructField("status", StringType(), True),
  ]),
  "sales_line": StructType([
      StructField("sales_order_id", IntegerType(), True),
      StructField("line_no", IntegerType(), True),
      StructField("item_id", IntegerType(), True),
      StructField("quantity", IntegerType(), True),
      StructField("unit_price", DoubleType(), True),
  ]),
  "item_ledger_entry": StructType([
      StructField("entry_id", IntegerType(), True),
      StructField("item_id", IntegerType(), True),
      StructField("location_code", StringType(), True),
      StructField("quantity", IntegerType(), True),
      StructField("posting_date", DateType(), True),
  ]),
  "value_entry": StructType([
      StructField("entry_id", IntegerType(), True),
      StructField("item_id", IntegerType(), True),
      StructField("cost_amount", DoubleType(), True),
      StructField("posting_date", DateType(), True),
  ]),
}


# COMMAND ----------

spark.sql("USE CATALOG erp_demo")
spark.sql("USE SCHEMA bronze")

base_path = "/Volumes/erp_demo/bronze/landing"

tables = {
  "customer": "customer.csv",
  "vendor": "vendor.csv",
  "item": "item.csv",
  "item_category": "item_category.csv",
  "location": "location.csv",
  "unit_of_measure": "unit_of_measure.csv",
  "currency": "currency.csv",
  "gl_account": "gl_account.csv",
  "gl_entry": "gl_entry.csv",
  "customer_ledger_entry": "customer_ledger_entry.csv",
  "vendor_ledger_entry": "vendor_ledger_entry.csv",
  "sales_header": "sales_header.csv",
  "sales_line": "sales_line.csv",
  "item_ledger_entry": "item_ledger_entry.csv",
  "value_entry": "value_entry.csv"
}

for table, file in tables.items():
    df = (
        spark.read
        .option("header", True)
        .option("dateFormat", "yyyy-MM-dd")
        .schema(schemas[table])
        .csv(f"{base_path}/{file}")
    )

    full_name = f"erp_demo.bronze.bronze_{table}"
    df.write.format("delta").mode("overwrite").saveAsTable(full_name)

print("✅ Typed Bronze tables created in erp_demo.bronze")


# COMMAND ----------

spark.table("erp_demo.bronze.bronze_customer").printSchema()
