# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from delta.tables import *

# COMMAND ----------

sorted_files = sorted(dbutils.fs.ls('/mnt/ci-mvi/Processed/SVeteran.SMVIPersonSiteAssociation/'), key=lambda x: x.modificationTime)
last_modification_time = sorted_files[-1].modificationTime

files_to_process = []

# for file in sorted_files:
#     if file.modificationTime > unix_process_start_time:
#         files_to_process.append(file.path)
#     else:
#         pass

# COMMAND ----------

print(last_modification_time)

# COMMAND ----------

if DeltaTable.isDeltaTable(spark, "/mnt/Patronage/identity_correlations") is False:

    status = "rebuild"

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
            person["MVITreatingFacilityInstitutionSID"]
            == institution["MVIInstitutionSID"],
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
        .withColumnRenamed("count", "count_1")
    )

    duplicate_icns = (
        relationship_df.groupBy("MVIInstitutionSID", "TreatingFacilityPersonIdentifier")
        .count()
        .filter(col("count") > 1)
        .withColumnRenamed("MVIInstitutionSID", "InstitutionSID")
        .withColumnRenamed("TreatingFacilityPersonIdentifier", "PersonIdentifier")
        .withColumnRenamed("count", "count_2")
    )

    unduped_relationship_df = (
        relationship_df.join(
            duplicate_iens,
            (relationship_df["MVIPersonICN"] == duplicate_iens["ICN"])
            & (
                relationship_df["MVIInstitutionSID"] == duplicate_iens["InstitutionSID"]
            ),
            "left",
        )
        .join(
            duplicate_icns,
            (relationship_df["MVIInstitutionSID"] == duplicate_icns["InstitutionSID"])
            & (
                relationship_df["TreatingFacilityPersonIdentifier"]
                == duplicate_icns["PersonIdentifier"]
            ),
            "left",
        )
        .filter(col("count_1").isNull())
        .filter(col("count_2").isNull())
        .select(
            relationship_df["MVIPersonICN"],
            relationship_df["InstitutionCode"],
            relationship_df["MVIInstitutionSID"],
            relationship_df["TreatingFacilityPersonIdentifier"],
            relationship_df["Last_Modified"],
        )
    )

    relationship_df.createOrReplaceTempView("relationship_df")
    unduped_relationship_df.createOrReplaceTempView("unduped_relationship_df")

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

    icn_master.write.format("delta").mode("overwrite").save(
        "/mnt/Patronage/identity_correlations"
    )
    
else:
    status = "update"
    pass

print(status)

# COMMAND ----------

if status = 'update':
    pass
else:
    pass
