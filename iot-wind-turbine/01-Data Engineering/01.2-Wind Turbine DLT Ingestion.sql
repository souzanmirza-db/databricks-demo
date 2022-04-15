-- Databricks notebook source
-- MAGIC %md 
-- MAGIC # Introducing Delta Live Tables
-- MAGIC ### A simple way to build and manage data pipelines for fresh, high quality data!
-- MAGIC 
-- MAGIC **Accelerate ETL development** <br/>
-- MAGIC Enable analysts and data engineers to innovate rapidly with simple pipeline development and maintenance 
-- MAGIC 
-- MAGIC **Remove operational complexity** <br/>
-- MAGIC By automating complex administrative tasks and gaining broader visibility into pipeline operations
-- MAGIC 
-- MAGIC **Trust your data** <br/>
-- MAGIC With built-in quality controls and quality monitoring to ensure accurate and useful BI, Data Science, and ML 
-- MAGIC 
-- MAGIC **Simplify batch and streaming** <br/>
-- MAGIC With self-optimization and auto-scaling data pipelines for batch or streaming processing 
-- MAGIC 
-- MAGIC ### Building a Delta Live Table pipeline to analyze and reduce churn
-- MAGIC 
-- MAGIC In this example, we'll implement a end 2 end DLT pipeline consuming our customers information.
-- MAGIC 
-- MAGIC We'll incrementally load new data with the autoloader, join this information and then load a model from MLFlow to perform our customer segmentation.
-- MAGIC 
-- MAGIC This information will then be used to build our DBSQL dashboard to track customer behavior and churn.
-- MAGIC 
-- MAGIC <div><img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/iot-wind-turbine/resources/images/turbine-demo-flow.png" width="90%"/></div>
-- MAGIC 
-- MAGIC <!-- do not remove -->
-- MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Fretail%2Fdlt%2Fnotebook_ingestion_sql&dt=DATA_PIPELINE">
-- MAGIC <!-- [metadata={"description":"Delta Live Table example in SQL. BRONZE/SILVER/GOLD. Expectations to track data quality. Load model from MLFLow registry and call it to apply customer segmentation as last step.<br/><i>Usage: basic DLT demo / Lakehouse presentation.</i>",
-- MAGIC  "authors":["quentin.ambard@databricks.com"],
-- MAGIC  "db_resources":{"DLT": ["DLT customer SQL"]},
-- MAGIC  "search_tags":{"vertical": "retail", "step": "Data Engineering", "components": ["autoloader", "copy into"]},
-- MAGIC  "canonicalUrl": {"AWS": "", "Azure": "", "GCP": ""}}] -->

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1/ Bronze layer: ingest data from Kafka

-- COMMAND ----------

-- DBTITLE 1,Use this one by default
CREATE INCREMENTAL LIVE TABLE users_bronze_dlt (
  CONSTRAINT correct_schema EXPECT (_rescued_data IS NULL)
)
COMMENT "raw turbine sensor data coming from json files ingested in incremental with Auto Loader to support schema inference and evolution"
AS SELECT * FROM cloud_files("/mnt/quentin-demo-resources/turbine/incoming-data", "parquet")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #Option 2, read from files instead
-- MAGIC # .maxFilesPerTrigger(1)
-- MAGIC 
-- MAGIC # bronzeDF = spark.readStream \
-- MAGIC #                 .format("cloudFiles") \
-- MAGIC #                 .option("cloudFiles.format", "parquet") \
-- MAGIC #                 .option("cloudFiles.maxFilesPerTrigger", 1) \
-- MAGIC #                 .schema("value string, key double") \
-- MAGIC #                 .load("/mnt/quentin-demo-resources/turbine/incoming-data") 
-- MAGIC 
-- MAGIC # bronzeDF.writeStream \
-- MAGIC #         .option("ignoreChanges", "true") \
-- MAGIC #         .trigger(processingTime='10 seconds') \
-- MAGIC #         .table("turbine_bronze")

-- COMMAND ----------

-- DBTITLE 1,Our raw data is now available in a Delta table, without having small files issues & with great performances
-- MAGIC %sql
-- MAGIC select * from turbine_bronze;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2/ Silver layer: transform JSON data into tabular table

-- COMMAND ----------

-- MAGIC %python
-- MAGIC jsonSchema = StructType([StructField(col, DoubleType(), False) for col in ["AN3", "AN4", "AN5", "AN6", "AN7", "AN8", "AN9", "AN10", "SPEED", "TORQUE", "ID"]] + [StructField("TIMESTAMP", TimestampType())])
-- MAGIC 
-- MAGIC spark.readStream.table('turbine_bronze') \
-- MAGIC      .withColumn("jsonData", from_json(col("value"), jsonSchema)) \
-- MAGIC      .select("jsonData.*") \
-- MAGIC      .writeStream \
-- MAGIC      .option("ignoreChanges", "true") \
-- MAGIC      .format("delta") \
-- MAGIC      .trigger(processingTime='10 seconds') \
-- MAGIC      .table("turbine_silver")

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC -- let's add some constraints in our table, to ensure or ID can't be negative (need DBR 7.5)
-- MAGIC ALTER TABLE turbine_silver ADD CONSTRAINT idGreaterThanZero CHECK (id >= 0);
-- MAGIC -- let's enable the auto-compaction
-- MAGIC alter table turbine_silver set tblproperties ('delta.autoOptimize.autoCompact' = true, 'delta.autoOptimize.optimizeWrite' = true);
-- MAGIC 
-- MAGIC -- Select data
-- MAGIC select * from turbine_silver;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3/ Gold layer: join information on Turbine status to add a label to our dataset

-- COMMAND ----------

-- MAGIC %sql 
-- MAGIC create table if not exists turbine_status_gold (id int, status string) using delta;
-- MAGIC 
-- MAGIC COPY INTO turbine_status_gold
-- MAGIC   FROM '/mnt/quentin-demo-resources/turbine/status'
-- MAGIC   FILEFORMAT = PARQUET;

-- COMMAND ----------

-- MAGIC %sql select * from turbine_status_gold

-- COMMAND ----------

-- DBTITLE 1,Join data with turbine status (Damaged or Healthy)
-- MAGIC %python
-- MAGIC turbine_stream = spark.readStream.table('turbine_silver')
-- MAGIC turbine_status = spark.read.table("turbine_status_gold")
-- MAGIC 
-- MAGIC turbine_stream.join(turbine_status, ['id'], 'left') \
-- MAGIC               .writeStream \
-- MAGIC               .option("ignoreChanges", "true") \
-- MAGIC               .format("delta") \
-- MAGIC               .trigger(processingTime='10 seconds') \
-- MAGIC               .table("turbine_gold")

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select * from turbine_gold;

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ## Run DELETE/UPDATE/MERGE with DELTA ! 
-- MAGIC We just realized that something is wrong in the data before 2020! Let's DELETE all this data from our gold table as we don't want to have wrong value in our dataset

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC DELETE FROM turbine_gold where timestamp < '2020-00-01';

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC -- DESCRIBE HISTORY turbine_gold;
-- MAGIC -- If needed, we can go back in time to select a specific version or timestamp
-- MAGIC SELECT * FROM turbine_gold TIMESTAMP AS OF '2020-12-01'
-- MAGIC 
-- MAGIC -- And restore a given version
-- MAGIC -- RESTORE turbine_gold TO TIMESTAMP AS OF '2020-12-01'
-- MAGIC 
-- MAGIC -- Or clone the table (zero copy)
-- MAGIC -- CREATE TABLE turbine_gold_clone [SHALLOW | DEEP] CLONE turbine_gold VERSION AS OF 32

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ## Our data is ready! Let's create a dashboard to monitor our Turbine plant using Databricks SQL Analytics
-- MAGIC 
-- MAGIC 
-- MAGIC ![turbine-demo-dashboard](https://github.com/QuentinAmbard/databricks-demo/raw/main/iot-wind-turbine/resources/images/turbine-demo-dashboard1.png)
-- MAGIC 
-- MAGIC [Open SQL Analytics dashboard](https://e2-demo-west.cloud.databricks.com/sql/dashboards/a81f8008-17bf-4d68-8c79-172b71d80bf0-turbine-demo?o=2556758628403379)
