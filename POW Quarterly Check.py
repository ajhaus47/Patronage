# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# COMMAND ----------

Window_Spec = Window.partitionBy(
    "MVIPersonICN",
    "MVITreatingFacilityInstitutionSID",
    "TreatingFacilityPersonIdentifier",
).orderBy(desc("CorrelationModifiedDateTime"))

person = (
    spark.read.parquet("/mnt/ci-mvi/Processed/SVeteran.SMVIPersonSiteAssociation/")
    .withColumn("rank", rank().over(Window_Spec))
    .withColumnRenamed("CorrelationModifiedDateTime", "Last_Modified")
    .filter(
        (
            (col("ActiveMergedIdentifierCode") == "A")
            | (col("ActiveMergedIdentifierCode").isNull())
        )
        & (col("OpCode").isin("I", "U"))
        & (col("POWFlag") == "Y")
    )
    .select(
        "MVIPersonICN",
        "Last_Modified",
        "POWFlag",
        "rank",
    )
    .filter(col("rank") == 1)
)

person.createOrReplaceTempView("person")

icn_relationship = spark.read.format('delta').load("/mnt/Patronage/identity_correlations")
scd_staging = spark.read.format('delta').load("/mnt/Patronage/SCD_Staging")

person_relationship = person.join(icn_relationship, "MVIPersonICN", "left")
person_relationship.createOrReplaceTempView('person_relationship')
scd_staging.createOrReplaceTempView('scd_staging')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select count(*) from person_relationship where MVIPersonICN NOT IN (select ICN from scd_staging)
