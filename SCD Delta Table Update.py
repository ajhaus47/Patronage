# Databricks notebook source
from pyspark.sql.functions import col, lit, when, desc, rank, lpad
from pyspark.sql.types import *
from pyspark.sql import Row
from datetime import datetime
from delta.tables import *
from pyspark.sql.window import Window

# COMMAND ----------

scd_schema = StructType([
    StructField('PTCPNT_ID', IntegerType()), 
    StructField('FILE_NBR', DoubleType()), 
    StructField('LAST_NM', StringType()), 
    StructField('FIRST_NM', StringType()), 
    StructField('MIDDLE_NM', StringType()), 
    StructField('SUFFIX_NM', StringType()), 
    StructField('STA_NBR', IntegerType()), 
    StructField('BRANCH_OF_SERV', StringType()), 
    StructField('DATE_OF_BIRTH', IntegerType()), 
    StructField('DATE_OF_DEATH', IntegerType()), 
    StructField('VET_SSN_NBR', IntegerType()), 
    StructField('SVC_NBR', StringType()), 
    StructField('AMT_GROSS_OR_NET_AWARD', IntegerType()),
    StructField('AMT_NET_AWARD', IntegerType()),
    StructField('NET_AWARD_DATE', IntegerType()), 
    StructField('SPECL_LAW_IND', IntegerType()), 
    StructField('VET_SSN_VRFCTN_IND', IntegerType()),
    StructField('WIDOW_SSN_VRFCTN_IND', IntegerType()), 
    StructField('PAYEE_SSN', IntegerType()), 
    StructField('ADDRS_ONE_TEXT', StringType()), 
    StructField('ADDRS_TWO_TEXT', StringType()),
    StructField('ADDRS_THREE_TEXT', StringType()), 
    StructField('ADDRS_CITY_NM', StringType()), 
    StructField('ADDRS_ST_CD', StringType()), 
    StructField('ADDRS_ZIP_PREFIX_NBR', IntegerType()), 
    StructField('MIL_POST_OFFICE_TYP_CD', StringType()), 
    StructField('MIL_POSTAL_TYPE_CD', StringType()), 
    StructField('COUNTRY_TYPE_CODE', IntegerType()), 
    StructField('SUSPENSE_IND', IntegerType()), 
    StructField('PAYEE_NBR', IntegerType()), 
    StructField('EOD_DT', IntegerType()), 
    StructField('RAD_DT', IntegerType()), 
    StructField('ADDTNL_SVC_IND', StringType()), 
    StructField('ENTLMT_CD', StringType()), 
    StructField('DSCHRG_PAY_GRADE_NM', StringType()), 
    StructField('AMT_OF_OTHER_RETIREMENT', IntegerType()), 
    StructField('RSRVST_IND', StringType()), 
    StructField('NBR_DAYS_ACTIVE_RESRV', IntegerType()), 
    StructField('CMBNED_DEGREE_DSBLTY', IntegerType()),
    StructField('DSBL_DTR_DT', IntegerType()), 
    StructField('DSBL_TYP_CD', StringType()),
    StructField('VA_SPCL_PROV_CD', IntegerType()),
  ])

vba_schema = StructType([
  StructField('EDI_PI', StringType()), 
  StructField('SSN_NBR', StringType()), 
  StructField('FILE_NBR', StringType()), 
  StructField('LAST_NM', StringType()), 
  StructField('FIRST_NM', IntegerType()), 
  StructField('MIDDLE_NM', StringType()),
  StructField('PAYEE_TYPE_CD', StringType()), 
  StructField('PT35_RATING_DT', StringType()), 
  StructField('PT35_PRMLGN_DT', StringType()), 
  StructField('EFFECTIVE_DATE', StringType()), 
  StructField('END_DT', StringType()), 
  StructField('PT_35_FLAG', StringType()), 
  StructField('COMBND_DEGREE_PCT', IntegerType()), 
  StructField('ICN', IntegerType()), 
  StructField('EDIPI', IntegerType()), 
  StructField('PARTICIPANT_ID', StringType()), 
])

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Initial Seed Processing

# COMMAND ----------

initial_seed = spark.read.csv("/mnt/ci-vba/PATRONAGE_REQ_OUTPUT1_20231129a.csv", header=True, inferSchema=True)\
    .withColumn("Batch_CD", lit("SCD"))\
    .withColumn("Individual_Unemployability", lit(None).cast("string"))\
    .withColumn("Status_Begin_Date", lit(None).cast("string"))\
    .withColumn("Status_Last_Update", lit(None).cast("string"))\
    .withColumn("Status_Termination_Date", lit(None).cast("string"))\
    .withColumn("SDP_Event_Created_Timestamp", lit(datetime(2023, 11, 29, 0, 0, 0)))

pai = spark.read.csv("/FileStore/df/Disabled_Vets_12_14_2023.csv", schema=vba_schema)\
    .withColumnRenamed('COMBND_DEGREE_PCT', 'SC_Combined_Disability_Percentage')\
    .withColumnRenamed('PT_35_FLAG', 'PT_Indicator')

icn_relationship = spark.read.format("delta").load("/mnt/Patronage/identity_correlations")\
    .withColumnRenamed('MVIPersonICN', 'ICN')\

scd_data = initial_seed\
.join(icn_relationship, initial_seed.PTCPNT_VET_ID == icn_relationship.participant_id, "left")\
.join(pai, initial_seed.PTCPNT_VET_ID == pai.PARTICIPANT_ID, "left")\
.select(icn_relationship.edipi, icn_relationship.ICN, initial_seed.Batch_CD, lpad(pai.SC_Combined_Disability_Percentage, 3, '0').alias('SC_Combined_Disability_Percentage'), pai.PT_Indicator, initial_seed.Individual_Unemployability, initial_seed.Status_Begin_Date, initial_seed.Status_Last_Update, initial_seed.Status_Termination_Date, initial_seed.SDP_Event_Created_Timestamp)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Write to Delta

# COMMAND ----------

scd_data.write.format("delta").mode("overwrite").save(
        "/mnt/Patronage/SCD_Staging"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Update Processing

# COMMAND ----------

Window_Spec = Window.partitionBy("participant_id").orderBy(desc("DSBL_DTR_DT"))

update_file = (
    spark.read.csv("/mnt/ci-vadir-shared/CPIDODIEX_202312_spool.csv", schema=scd_schema)
    .withColumnRenamed("PTCPNT_ID", "participant_id")
    .withColumn("rank", rank().over(Window_Spec))
    .withColumn("Batch_CD", lit("SCD"))
    .withColumnRenamed("CMBNED_DEGREE_DSBLTY", "SC_Combined_Disability_Percentage")
    .withColumnRenamed("DSBL_TYP_CD", "PT_Indicator")
    .withColumn("Individual_Unemployability", lit(None).cast("string"))
    .withColumn("Status_Begin_Date", lit(None).cast("string"))
    .withColumnRenamed("DSBL_DTR_DT", "Status_Last_Update")
    .withColumn("Status_Termination_Date", lit(None).cast("string"))
    .withColumn("SDP_Event_Created_Timestamp", lit(datetime(2023, 12, 31, 0, 0, 0)))
    .filter(col("rank") == 1)
    .select("participant_id", "Batch_CD", "SC_Combined_Disability_Percentage", "PT_Indicator", "Individual_Unemployability", "Status_Begin_Date", "Status_Last_Update", "Status_Termination_Date", "SDP_Event_Created_Timestamp")
)

duplicate_participant_ids = (
    update_file.groupBy("participant_id").count().filter(col("count") > 1).withColumnRenamed("participant_id", "pat_id").withColumnRenamed("count", "cnt")
)


update_dataframe = (
    update_file.join(
        icn_relationship,
        update_file.participant_id == icn_relationship.participant_id,
        "left",
    )
    .join(
        duplicate_participant_ids,
        update_file.participant_id == duplicate_participant_ids.pat_id,
        "left",
    )
    .filter(col("cnt").isNull())
    .select(
        icn_relationship.edipi,
        icn_relationship.ICN,
        icn_relationship.participant_id,
        update_file.Batch_CD,
        lpad(update_file.SC_Combined_Disability_Percentage, 3, '0').alias('SC_Combined_Disability_Percentage'),
        update_file.PT_Indicator,
        update_file.Individual_Unemployability,
        update_file.Status_Begin_Date,
        update_file.Status_Last_Update,
        update_file.Status_Termination_Date,
        update_file.SDP_Event_Created_Timestamp,
        duplicate_participant_ids.cnt
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Merge To Delta

# COMMAND ----------

SCDDeltaTable = DeltaTable.forPath(spark, "/mnt/Patronage/SCD_Staging")

# COMMAND ----------

SCDDeltaTable.alias("master").merge(
    update_dataframe.alias("update"),
    "master.edipi = update.edipi",
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
