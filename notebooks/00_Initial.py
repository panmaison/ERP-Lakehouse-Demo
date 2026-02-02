# Databricks notebook source
# MAGIC %sql
# MAGIC create catalog if not exists erp_demo

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS erp_demo.bronze;
# MAGIC CREATE SCHEMA IF NOT EXISTS erp_demo.silver;
# MAGIC CREATE SCHEMA IF NOT EXISTS erp_demo.gold;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS erp_demo.bronze.landing;

# COMMAND ----------

dbutils.fs.ls("/Volumes/erp_demo/bronze/landing")
