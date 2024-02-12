# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from datetime import datetime
from delta.tables import *

# COMMAND ----------

sorted_files = sorted(dbutils.fs.ls('/mnt/ci-mvi/Processed/SVeteran.SMVIPersonSiteAssociation/'), key=lambda x: x.modificationTime)
latest_modification_time = sorted_files[-1].modificationTime
date_today = datetime.today().date()

ICUpdateRuns = spark.read.format("delta").load("/mnt/Patronage/Identity_Correlation_Run_History")
ICUpdateRuns.createOrReplaceTempView('ICUpdateRuns_View')

institution = spark.read.parquet("/mnt/ci-mvi/Raw/NDim.MVIInstitution/")

Window_Spec = Window.partitionBy(
        "MVIPersonICN",
        "MVITreatingFacilityInstitutionSID",
        "TreatingFacilityPersonIdentifier",
    ).orderBy(desc("CorrelationModifiedDateTime"))

# COMMAND ----------

if DeltaTable.isDeltaTable(spark, "/mnt/Patronage/identity_correlations") is False:
    status = "rebuild"
    files_to_process = sorted_files
else:
    status = 'update'
    unix_process_start_time  = spark.sql('select latest_modification_time from ICUpdateRuns_View order by latest_modification_time desc limit 1').collect()[0][0] 
    files_to_process = []
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

columns = ["run_date", "number_of_buckets", "latest_modification_time"]
data = [(date_today, len(files_to_process), latest_modification_time)]
rdd = spark.sparkContext.parallelize(data)
df = rdd.toDF(columns)

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

if status == 'update':
    identity_correlations = spark.read.format("delta").load("/mnt/Patronage/identity_correlations")

    participant_id = unduped_relationship_df.filter(col("InstitutionCode") == '200CORP')
    edipi = unduped_relationship_df.filter(col("InstitutionCode") == '200DOD')
    va_profile_id = unduped_relationship_df.filter(col("InstitutionCode") == '200VETS')
else:
    pass

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select MVIPersonICN, InstitutionCode, count(*) from relationship_df group by 1, 2 order by 3 desc;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from relationship_df where MVIPersonICN = 1008856884 and InstitutionCode = '200CORP'

# COMMAND ----------

display(spark.read.format('delta').load("/mnt/Patronage/CG_Run_History"))
