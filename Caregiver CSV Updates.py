# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ###Read Delta Table History

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC DESCRIBE HISTORY '/mnt/Patronage/Caregivers_Staging' 

# COMMAND ----------

new_version = spark.read.format("delta").load("/mnt/Patronage/Caregivers_Staging@v1")
old_version = spark.read.format("delta").load("/mnt/Patronage/Caregivers_Staging@v0")

new_version.createOrReplaceTempView('new_version')
old_version.createOrReplaceTempView('old_version')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ###Pull Changes Between Versions

# COMMAND ----------

updates = spark.sql("select * from new_version except all select * from old_version")

# COMMAND ----------

display(updates)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Save As CSV to Send

# COMMAND ----------

import datetime

# COMMAND ----------

date_today = datetime.datetime.now().date()

updates.coalesce(1).write.format("com.databricks.spark.csv").option(
    "header", "true",
).save(f"dbfs:/FileStore/df/{str(date_today).replace('-', '')}PatronageCGChangeFile.csv")

# COMMAND ----------


