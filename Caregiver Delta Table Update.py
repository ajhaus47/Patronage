# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ###Initializing

# COMMAND ----------

pip install openpyxl

# COMMAND ----------

import pandas as pd
from datetime import datetime
from delta.tables import *
from pyspark.sql.types import *
from pyspark.sql.functions import lit, col, current_timestamp, current_date, max

# COMMAND ----------

def equivalent_type(f):
    if f == 'datetime64[ns]': return TimestampType()
    elif f == 'int64': return LongType()
    elif f == 'int32': return IntegerType()
    elif f == 'float64': return DoubleType()
    elif f == 'float32': return FloatType()
    else: return StringType()

def define_structure(string, format_type):
    try: typo = equivalent_type(format_type)
    except: typo = StringType()
    return StructField(string, typo)

def pandas_to_spark(pandas_df):
    columns = list(pandas_df.columns)
    types = list(pandas_df.dtypes)
    struct_list = []
    for column, typo in zip(columns, types): 
      struct_list.append(define_structure(column, typo))
    p_schema = StructType(struct_list)
    return sqlContext.createDataFrame(pandas_df, p_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ###Register EDIPI Source

# COMMAND ----------

person_site_associations_5667 = (
    spark.read.parquet("/mnt/ci-mvi/Processed/SVeteran.SMVIPersonSiteAssociation/")
    .filter(
        (col("MVITreatingFacilityInstitutionSID") == 5667)
        & (
            (col("ActiveMergedIdentifierCode") == "A")
            | col("ActiveMergedIdentifierCode").isNull()
        )
    )
    .select("EDIPI", "MVIPersonICN", "calc_IngestionTimestamp")
    .groupBy("EDIPI", "MVIPersonICN")
    .agg(max("calc_IngestionTimestamp").alias("IngestionTimeStamp"))
)

person_site_associations_5667.createOrReplaceTempView("person_site_associations_5667")

final_edipi = spark.sql(
    "select EDIPI, MVIPersonICN, IngestionTimeStamp, RANK() OVER (PARTITION BY EDIPI ORDER BY IngestionTimeStamp desc) as rank from person_site_associations_5667"
)

final_edipi.createOrReplaceTempView("final_edipi")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ###Process Initial Seed Data

# COMMAND ----------

if DeltaTable.isDeltaTable(spark, "/mnt/Patronage/Caregivers_Staging") is False:
    status = "rebuild"
    filePath = "/dbfs/FileStore/CGs31October2023.xlsx"
    df = pd.read_excel(filePath, engine="openpyxl")

    df_CareGivers = (
        pandas_to_spark(df)
        .withColumn("CG_ICN", col("Person ICN").substr(1, 10))
        .withColumn("Veteran_ICN", col("CARMA Case Details: Veteran ICN").substr(1, 10))
        .withColumn("Batch_CD", lit("CG"))
        .withColumn("SC_Combined_Disability_Percentage", lit(None).cast("int"))
        .withColumn("PT_Indicator", lit(None).cast("string"))
        .withColumn("Individual_Unemployability", lit(None).cast("string"))
        .withColumn("Status_Last_Update", lit(None).cast("string"))
        .withColumnRenamed("Dispositioned Date", "Status_Begin_Date")
        .withColumnRenamed("Benefits End Date", "Status_Termination_Date")
        .withColumnRenamed("Caregiver Status", "Status")
        .withColumn("SDP_Event_Created_Timestamp", lit(datetime(2023, 10, 31, 0, 0, 0)))
        .drop(
            "Person ICN",
            "CARMA Case Details: Veteran ICN",
            "Applicant Type",
        )
        .filter(
            (col("Status").isin(["Approved", "Pending Revocation/Discharge"]))
            | (col("Status_Termination_Date") >= current_date())
            | (col("Status_Termination_Date").isNull())
        )
    )

    df_CareGivers.createOrReplaceTempView("Caregivers")

    Caregivers = spark.sql(
        "select mvi.EDIPI as EDIPI, CG_ICN as ICN, cg.Batch_CD, cg.SC_Combined_Disability_Percentage, cg.PT_Indicator as PT_Indicator, cg.Individual_Unemployability, cg.Status_Begin_Date, cg.Status_Last_Update, cg.Status_Termination_Date, cg.Status, cg.SDP_Event_Created_Timestamp from CareGivers cg left join final_edipi mvi on cg.CG_ICN = mvi.MVIPersonICN where mvi.rank = 1"
    )

    Caregivers.write.format("delta").mode("overwrite").save(
        "/mnt/Patronage/Caregivers_Staging"
    )
else:
    status = "update"
    pass

print(status)

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ### Read Delta Table

# COMMAND ----------

CGDeltaTable = DeltaTable.forPath(spark, "/mnt/Patronage/Caregivers_Staging")
CGRunUpdateTable = DeltaTable.forPath(spark, "/mnt/Patronage/CG_Run_History")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ###Process Upsert Files

# COMMAND ----------

CGUpdateRuns = spark.read.format("delta").load("/mnt/Patronage/CG_Run_History")
CGUpdateRuns.createOrReplaceTempView('CGUpdateRuns_View')

now = datetime.now()
now_unix = datetime.timestamp(now)

if status == 'rebuild':
    process_start_time  = datetime(2023, 10, 31) 
    unix_process_start_time = datetime.timestamp(process_start_time)*1000
else:
    unix_process_start_time  = spark.sql('select modification_time from CGUpdateRuns_View order by modification_time desc limit 1').collect()[0][0] 
    
files = dbutils.fs.ls('/mnt/ci-carma/landing')
sorted_files = sorted(files, key=lambda x: x.modificationTime)

# COMMAND ----------

files_to_process = []

for file in files:
    if ('csv' in file.path) & (file.modificationTime > unix_process_start_time):
        files_to_process.append(file.path)
    else:
        pass
    

# COMMAND ----------

upsert_df = (
    spark.read.csv(files_to_process, header=True, inferSchema=True)
    .select(
        "Caregiver_ICN__c",
        "Applicant_Type__c",
        "Caregiver_Status__c",
        "Dispositioned_Date__c",
        "Benefits_End_Date__c",
        "Veteran_ICN__c",
        "CreatedDate",
    )
    .withColumn("CG_ICN", col("Caregiver_ICN__c").substr(1, 10))
    .withColumn("Veteran_ICN", col("Veteran_ICN__c").substr(1, 10))
    .withColumn("Batch_CD", lit("CG"))
    .withColumn("SC_Combined_Disability_Percentage", lit(None).cast("int"))
    .withColumn("PT_Indicator", lit(None).cast("string"))
    .withColumn("Individual_Unemployability", lit(None).cast("string"))
    .withColumn("Status_Last_Update", lit(None).cast("string"))
    .withColumnRenamed("Caregiver_Status__c", "Status")
    .withColumnRenamed("Dispositioned_Date__c", "Status_Begin_Date")
    .withColumnRenamed("Benefits_End_Date__c", "Status_Termination_Date")
    .withColumnRenamed("CreatedDate", "Event_Created_Date")
    .filter(
        (col("Status").isin(["Approved", "Pending Revocation/Discharge"]))
        | (col("Status_Termination_Date") >= current_date())
        | (col("Status_Termination_Date").isNull())
    )
    .filter(col("Applicant_Type__c") == "Primary Caregiver")
    .drop(
        "Caregiver_ICN__c", "Veteran_ICN__c", "Applicant_Type__c", "Caregiver_Status__c"
    )
)

upsert_df.createOrReplaceTempView("upsert_df")

upsert_agg_df = spark.sql(
    "with window AS (select CG_ICN, Veteran_ICN, Batch_CD, SC_Combined_Disability_Percentage, PT_Indicator, Individual_Unemployability, Status_Last_Update, Status_Begin_Date, Status_Termination_Date, Status, Event_Created_Date, RANK() OVER (PARTITION BY CG_ICN, Veteran_ICN ORDER BY Event_Created_Date desc) as rank from upsert_df) select *, count(*) as count from window where rank = 1 group by all"
)
upsert_agg_df.createOrReplaceTempView("upsert_agg_df")

edipi_df = spark.sql(
    "with window AS (select mvi.EDIPI as EDIPI, CG_ICN as ICN, cg.Batch_CD, cg.SC_Combined_Disability_Percentage, cg.PT_Indicator as PT_Indicator, cg.Individual_Unemployability, cg.Status_Begin_Date, cg.Status_Last_Update, cg.Status_Termination_Date, cg.Status, cg.Event_Created_Date as SDP_Event_Created_Timestamp, RANK() OVER (PARTITION BY EDIPI, CG_ICN ORDER BY cg.Event_Created_Date desc) as rank from upsert_agg_df cg left join final_edipi mvi on cg.CG_ICN = mvi.MVIPersonICN where mvi.rank = 1) select * from window where rank = 1"
)

CGDeltaTable.alias("master").merge(
    edipi_df.alias("update"),
    "master.EDIPI = update.EDIPI",
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ###Update Run History

# COMMAND ----------

last_modification_time = sorted_files[-1].modificationTime
number_of_upserts = len(files_to_process)
upsert_rows = upsert_df.count()
deduped_upsert_rows = upsert_agg_df.count()
date_today = datetime.today().date()

update_row = [
    Row(date_today, last_modification_time, number_of_upserts, upsert_rows, deduped_upsert_rows)
]
update_columns = ["run_date", "modification_time", "number_of_upserts", "upsert_rows", "deduped_upsert_rows"]

update_df = spark.createDataFrame(update_row).toDF(*update_columns)

CGRunUpdateTable.alias("master").merge(update_df.alias("update"), "master.modification_time = update.modification_time").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# COMMAND ----------

display(CGUpdateRuns)
