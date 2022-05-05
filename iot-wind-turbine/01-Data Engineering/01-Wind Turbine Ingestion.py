# Databricks notebook source
dbutils.widgets.dropdown("reset_all_data", "false", ["true", "false"], "Reset all data")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC # Wind Turbine Predictive Maintenance
# MAGIC 
# MAGIC In this example, we demonstrate anomaly detection for the purposes of finding damaged wind turbines. A damaged, single, inactive wind turbine costs energy utility companies thousands of dollars per day in losses.
# MAGIC 
# MAGIC 
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/iot-wind-turbine/resources/images/turbine-demo-flow.png" width="90%"/>
# MAGIC 
# MAGIC 
# MAGIC <div style="float:right; margin: -10px 50px 0px 50px">
# MAGIC   <img src="https://s3.us-east-2.amazonaws.com/databricks-knowledge-repo-images/ML/wind_turbine/wind_small.png" width="400px" /><br/>
# MAGIC   *locations of the sensors*
# MAGIC </div>
# MAGIC Our dataset consists of vibration readings coming off sensors located in the gearboxes of wind turbines. 
# MAGIC 
# MAGIC We will use Gradient Boosted Tree Classification to predict which set of vibrations could be indicative of a failure.
# MAGIC 
# MAGIC One the model is trained, we'll use MFLow to track its performance and save it in the registry to deploy it in production
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC *Data Source Acknowledgement: This Data Source Provided By NREL*
# MAGIC 
# MAGIC *https://www.nrel.gov/docs/fy12osti/54530.pdf*

# COMMAND ----------

# DBTITLE 1,Let's prepare our data first
# MAGIC %run ../resources/00-setup $reset_all_data=$reset_all_data

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1/ Bronze layer: ingest data from Kafka

# COMMAND ----------

# DBTITLE 1,Let's explore what is being delivered by our wind turbines stream: (key, json)
# MAGIC %sql 
# MAGIC select * from parquet.`/mnt/databricks-souzan-field-demo/turbine/incoming-data`

# COMMAND ----------

spark.read.parquet('/mnt/databricks-souzan-field-demo/turbine/incoming-data').printSchema()

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table turbine_bronze (key long not null, value string) using delta ;
# MAGIC   
# MAGIC -- Turn on autocompaction to solve small files issues on your streaming job, that's all you have to do!
# MAGIC alter table turbine_bronze set tblproperties ('delta.autoOptimize.autoCompact' = true, 'delta.autoOptimize.optimizeWrite' = true);

# COMMAND ----------

# DBTITLE 1,Use ONLY if Kafka stream has been setup on AWS/Kinesis
#Option 1, read from kinesis directly
#Load stream from Kafka
bronzeDF = spark.readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", "kafkaserver1:9092,kafkaserver2:9092") \
                .option("subscribe", "turbine") \
                .load()

#Write the output to a delta table
bronzeDF.selectExpr("CAST(key AS STRING) as key", "CAST(value AS STRING) as value") \
        .writeStream \
        .option("ignoreChanges", "true") \
        .trigger(once=True) \
        .table("turbine_bronze")

# COMMAND ----------

# DBTITLE 1,Use this one by default
#Option 2, read from files instead
bronzeDF = spark.readStream \
                .format("cloudFiles") \
                .option("cloudFiles.format", "parquet") \
                .option("cloudFiles.maxFilesPerTrigger", 1) \
                .schema("value string, key long") \
                .load("/mnt/databricks-souzan-field-demo/turbine/incoming-data") 

bronzeDF.writeStream \
        .option("ignoreChanges", "true") \
        .trigger(processingTime='10 seconds') \
        .table("turbine_bronze")

# COMMAND ----------

# DBTITLE 1,Our raw data is now available in a Delta table, without having small files issues & with great performances
# MAGIC %sql
# MAGIC select * from turbine_bronze;

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table power_bronze 
# MAGIC (
# MAGIC   date timestamp,
# MAGIC   power double, 
# MAGIC   theoretical_power_curve double, 
# MAGIC   turbine_id bigint, 
# MAGIC   wind_direction double, 
# MAGIC   wind_speed double) 
# MAGIC   using delta ;
# MAGIC   
# MAGIC -- Turn on autocompaction to solve small files issues on your streaming job, that's all you have to do!
# MAGIC alter table power_bronze set tblproperties ('delta.autoOptimize.autoCompact' = true, 'delta.autoOptimize.optimizeWrite' = true);

# COMMAND ----------

# DBTITLE 1,Read in Power data and infer schema
#Option 2, read from files instead
bronze_powerDF = (spark.readStream 
                .format("cloudFiles") 
                .option("cloudFiles.format", "json") 
                .option("cloudFiles.maxFilesPerTrigger", 1) 
                .option("cloudfiles.schemaHints", "date timestamp, power double, theoretical_power_curve double, turbine_id bigint, wind_direction double, wind_speed double")
                .option("cloudfiles.schemaLocation", path) 
                .load("/mnt/databricks-souzan-field-demo/turbine/power/raw") 
                 )


bronze_powerDF.writeStream \
        .option("ignoreChanges", "true") \
        .option("mergeSchema", "true") \
        .trigger(processingTime='10 seconds') \
        .table("power_bronze")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from power_bronze

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2/ Silver layer: transform JSON data into tabular table

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from turbine_bronze limit 10;

# COMMAND ----------

jsonSchema = StructType([StructField(col, DoubleType(), False) for col in ["AN3", "AN4", "AN5", "AN6", "AN7", "AN8", "AN9", "AN10", "SPEED", "TORQUE"]] + [StructField("TIMESTAMP", TimestampType())] + [StructField("ID", IntegerType())])


spark.readStream.table('turbine_bronze') \
     .withColumn("jsonData", from_json(col("value"), jsonSchema)) \
     .select("jsonData.*") \
     .writeStream \
     .option("ignoreChanges", "true") \
     .format("delta") \
     .trigger(processingTime='10 seconds') \
     .table("turbine_silver")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- let's add some constraints in our table, to ensure or ID can't be negative (need DBR 7.5)
# MAGIC ALTER TABLE turbine_silver ADD CONSTRAINT idGreaterThanZero CHECK (id >= 0);
# MAGIC -- let's enable the auto-compaction
# MAGIC alter table turbine_silver set tblproperties ('delta.autoOptimize.autoCompact' = true, 'delta.autoOptimize.optimizeWrite' = true);
# MAGIC 
# MAGIC -- Select data
# MAGIC select * from turbine_silver;

# COMMAND ----------

# DBTITLE 1,Create unix timestamp for average power calculation
spark.readStream.table('power_bronze') \
     .withColumn("timestamp", unix_timestamp(to_timestamp('date'))) \
     .writeStream \
     .option("ignoreChanges", "true") \
     .format("delta") \
     .trigger(processingTime='10 seconds') \
     .table("power_silver")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- let's add some constraints in our table, to ensure or ID can't be negative (need DBR 7.5)
# MAGIC ALTER TABLE power_silver ADD CONSTRAINT idGreaterThanZero CHECK (turbine_id >= 0);
# MAGIC -- let's enable the auto-compaction
# MAGIC alter table power_silver set tblproperties ('delta.autoOptimize.autoCompact' = true, 'delta.autoOptimize.optimizeWrite' = true);
# MAGIC 
# MAGIC -- Select data
# MAGIC select * from power_silver;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3/ Gold layer: join information on Turbine status to add a label to our dataset

# COMMAND ----------

# MAGIC %sql 
# MAGIC create table if not exists turbine_status_gold (id int, status string) using delta;
# MAGIC 
# MAGIC COPY INTO turbine_status_gold
# MAGIC   FROM '/mnt/databricks-souzan-field-demo/turbine/status'
# MAGIC   FILEFORMAT = PARQUET;

# COMMAND ----------

from pyspark.sql.functions import count, countDistinct

spark.read.table('turbine_status_gold').agg(count('id'), countDistinct('id')).show()

# COMMAND ----------

# DBTITLE 1,Join data with turbine status (Damaged or Healthy)
turbine_stream = spark.readStream.table('turbine_silver')
turbine_status = spark.read.table("turbine_status_gold")

turbine_stream.join(turbine_status, ['id'], 'left') \
              .writeStream \
              .option("ignoreChanges", "true") \
              .format("delta") \
              .trigger(processingTime='10 seconds') \
              .table("turbine_gold")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from turbine_gold;

# COMMAND ----------

# DBTITLE 1,Calculate average power in gold table
win = Window.partitionBy('turbine_id').orderBy("timestamp").rangeBetween(-7200, 0)

(spark.read.table('power_silver') 
     .withColumn("average_power", avg("power").over(win))  
     .write
     .option("ignoreChanges", "true") 
     .format("delta") 
     .mode("overwrite")    
     .saveAsTable("power_gold")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from power_gold;

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Run DELETE/UPDATE/MERGE with DELTA ! 
# MAGIC We just realized that something is wrong in the data before 2020! Let's DELETE all this data from our gold table as we don't want to have wrong value in our dataset

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM turbine_gold where timestamp < '2020-00-01';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DESCRIBE HISTORY turbine_gold;
# MAGIC -- If needed, we can go back in time to select a specific version or timestamp
# MAGIC SELECT * FROM turbine_gold TIMESTAMP AS OF '2020-12-01'
# MAGIC 
# MAGIC -- And restore a given version
# MAGIC -- RESTORE turbine_gold TO TIMESTAMP AS OF '2020-12-01'
# MAGIC 
# MAGIC -- Or clone the table (zero copy)
# MAGIC -- CREATE TABLE turbine_gold_clone [SHALLOW | DEEP] CLONE turbine_gold VERSION AS OF 32

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Our data is ready! Let's create a dashboard to monitor our Turbine plant using Databricks SQL Analytics
# MAGIC 
# MAGIC 
# MAGIC ![turbine-demo-dashboard](https://github.com/QuentinAmbard/databricks-demo/raw/main/iot-wind-turbine/resources/images/turbine-demo-dashboard1.png)
# MAGIC 
# MAGIC [Open SQL Analytics dashboard](https://e2-demo-west.cloud.databricks.com/sql/dashboards/a81f8008-17bf-4d68-8c79-172b71d80bf0-turbine-demo?o=2556758628403379)
