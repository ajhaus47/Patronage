# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from datetime import datetime
from delta.tables import *

# COMMAND ----------

sorted_files = sorted(
    dbutils.fs.ls("/mnt/ci-mvi/Processed/SVeteran.SMVIPersonSiteAssociation/"),
    key=lambda x: x.modificationTime,
)

ICUpdateRuns = spark.read.format("delta").load(
    "/mnt/Patronage/Identity_Correlation_Run_History"
)

ICUpdateRuns.createOrReplaceTempView("ICUpdateRuns_View")

institution = spark.read.parquet("/mnt/ci-mvi/Raw/NDim.MVIInstitution/")

Window_Spec = Window.partitionBy(
    "MVIPersonICN",
    "MVITreatingFacilityInstitutionSID",
    "TreatingFacilityPersonIdentifier",
).orderBy(desc("CorrelationModifiedDateTime"))

# COMMAND ----------

files_to_process = []

if DeltaTable.isDeltaTable(spark, "/mnt/Patronage/identity_correlations") is False:
    status = "rebuild"
    for file in sorted_files:
        files_to_process.append(file.path)
else:
    status = 'update'
    unix_process_start_time  = spark.sql('select latest_modification_time from ICUpdateRuns_View order by latest_modification_time desc limit 1').collect()[0][0] 
    for file in sorted_files:
        if file.modificationTime > unix_process_start_time:
            files_to_process.append(file.path)
        else:
            pass

print(status)

# COMMAND ----------

person = (
    spark.read.parquet(*files_to_process)
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

# COMMAND ----------

if status == 'rebuild':

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
    pass
    print('pass')

# COMMAND ----------

if status == "update":
    identity_correlations = DeltaTable.forPath(
        spark, "/mnt/Patronage/identity_correlations"
    )

    participant_id = unduped_relationship_df.filter(col("InstitutionCode") == "200CORP")
    edipi = unduped_relationship_df.filter(col("InstitutionCode") == "200DOD")
    va_profile_id = unduped_relationship_df.filter(col("InstitutionCode") == "200VETS")

    identity_correlations.alias("master").merge(
        participant_id.alias("update"),
        "master.MVIPersonICN = update.MVIPersonICN",
    ).whenMatchedUpdate(
        set={
            "MVIPersonICN": "master.MVIPersonICN",
            "participant_id": "update.TreatingFacilityPersonIdentifier",
            "edipi": "master.edipi",
            "va_profile_id": "master.va_profile_id",
        }
    ).whenNotMatchedInsert(
        values={
            "MVIPersonICN": "update.MVIPersonICN",
            "participant_id": "update.TreatingFacilityPersonIdentifier",
            "edipi": lit(None),
            "va_profile_id": lit(None),
        }
    ).execute()

    identity_correlations.alias("master").merge(
        edipi.alias("update"),
        "master.MVIPersonICN = update.MVIPersonICN",
    ).whenMatchedUpdate(
        set={
            "MVIPersonICN": "master.MVIPersonICN",
            "participant_id": "master.participant_id",
            "edipi": "update.TreatingFacilityPersonIdentifier",
            "va_profile_id": "master.va_profile_id",
        }
    ).whenNotMatchedInsert(
        values={
            "MVIPersonICN": "update.MVIPersonICN",
            "participant_id": lit(None),
            "edipi": "update.TreatingFacilityPersonIdentifier",
            "va_profile_id": lit(None),
        }
    ).execute()

    identity_correlations.alias("master").merge(
        va_profile_id.alias("update"),
        "master.MVIPersonICN = update.MVIPersonICN",
    ).whenMatchedUpdate(
        set={
            "MVIPersonICN": "master.MVIPersonICN",
            "participant_id": "master.participant_id",
            "edipi": "master.edipi",
            "va_profile_id": "update.TreatingFacilityPersonIdentifier",
        }
    ).whenNotMatchedInsert(
        values={
            "MVIPersonICN": "update.MVIPersonICN",
            "participant_id": lit(None),
            "edipi": lit(None),
            "va_profile_id": "update.TreatingFacilityPersonIdentifier",
        }
    ).execute()
    
else:
    pass
    print("pass")

# COMMAND ----------

identity_correlations = DeltaTable.forPath(
        spark, "/mnt/Patronage/Identity_Correlation_Run_History"
    )

latest_modification_time = sorted_files[-1].modificationTime
number_of_buckets = len(files_to_process)
date_today = datetime.today().date()

update_row = [
    Row(date_today, number_of_buckets, latest_modification_time)
]

update_columns = ["run_date", "number_of_buckets", "latest_modification_time"]

update_run_df = spark.createDataFrame(update_row).toDF(*update_columns)

identity_correlations.alias("master").merge(update_run_df.alias("update"), "master.run_date = update.run_date").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
