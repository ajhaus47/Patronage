# Databricks notebook source
cg_data = spark.read.format("delta").load("/mnt/Patronage/Caregivers_Staging")
scd_data = spark.read.format("delta").load("/mnt/Patronage/SCD_Staging")

cg_data.createOrReplaceTempView('cg_data')
scd_data.createOrReplaceTempView('scd_data')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from cg_data where SDP_Event_Created_Timestamp >= ''

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from scd_data where SDP_Event_Created_Timestamp >= ''
