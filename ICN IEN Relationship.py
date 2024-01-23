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
        & (col("MVITreatingFacilityInstitutionSID").isin(5667, 6061, 6722))
    )
    .select(
        "MVIPersonICN",
        "TreatingFacilityPersonIdentifier",
        "MVITreatingFacilityInstitutionSID",
        "calc_IngestionTimestamp",
        "Last_Modified",
        "rank",
    )
    .filter(col("rank") == 1)
)

institution = spark.read.parquet("/mnt/ci-mvi/Raw/NDim.MVIInstitution/")

relationship_df = (
    person.join(
        institution,
        person["MVITreatingFacilityInstitutionSID"] == institution["MVIInstitutionSID"],
        "left",
    )
    .select(
        person["MVIPersonICN"],
        institution["InstitutionCode"],
        institution["MVIInstitutionSID"],
        person["TreatingFacilityPersonIdentifier"],
        person["Last_Modified"],
    )
    .distinct()
)

duplicate_iens = (
    relationship_df.groupBy("MVIPersonICN", "MVIInstitutionSID")
    .count()
    .filter(col("count") > 1)
    .withColumnRenamed("MVIPersonICN", "ICN")
    .withColumnRenamed("MVIInstitutionSID", "InstitutionSID")
)

unduped_relationship_df = relationship_df\
    .join(duplicate_iens, (relationship_df["MVIPersonICN"] == duplicate_iens["ICN"]) & (relationship_df["MVIInstitutionSID"] == duplicate_iens["InstitutionSID"]), "left")\
    .filter(col("count").isNull())\
    .select(
        relationship_df["MVIPersonICN"], 
        relationship_df["InstitutionCode"],
        relationship_df["MVIInstitutionSID"], 
        relationship_df["TreatingFacilityPersonIdentifier"],
        relationship_df["Last_Modified"]
    )
        

relationship_df.createOrReplaceTempView("relationship_df")
unduped_relationship_df.createOrReplaceTempView("unduped_relationship_df")

# COMMAND ----------

display(unduped_relationship_df)

# COMMAND ----------

icn_master = spark.sql("""
with 200CORP as (
  select
    MVIPersonICN,
    TreatingFacilityPersonIdentifier, 
    Last_Modified
  from
    unduped_relationship_df
  where
    InstitutionCode = '200CORP'
),
200DOD as (
  select
    MVIPersonICN,
    TreatingFacilityPersonIdentifier,
    Last_Modified
  from
    unduped_relationship_df
  where
    InstitutionCode = '200DOD'
),
200VETS as (
  select
    MVIPersonICN,
    TreatingFacilityPersonIdentifier,
    Last_Modified
  from
    unduped_relationship_df
  where
    InstitutionCode = '200VETS'
),
unique_ICNs as (
  select distinct MVIPersonICN from relationship_df
)
select
  a.MVIPersonICN,
  b.TreatingFacilityPersonIdentifier as participant_id,
  c.TreatingFacilityPersonIdentifier as edipi,
  d.TreatingFacilityPersonIdentifier as va_profile_id
from
  unique_ICNs a
  left join 200CORP b on a.MVIPersonICN = b.MVIPersonICN
  left join 200DOD c on a.MVIPersonICN = c.MVIPersonICN
  left join 200VETS d on a.MVIPersonICN = d.MVIPersonICN
  """)

icn_master.createOrReplaceTempView('icn_master')

# COMMAND ----------

display(icn_master)

# COMMAND ----------

icn_master.write.format("delta").mode("overwrite").save(
        "/mnt/Patronage/identity_correlations"
    )
# write to ci-patronage (PUT Error)

# COMMAND ----------

