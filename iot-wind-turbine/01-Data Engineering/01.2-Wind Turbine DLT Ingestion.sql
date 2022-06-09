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
-- MAGIC ## 1/ Bronze layer: ingest data using cloud files

-- COMMAND ----------

CREATE INCREMENTAL LIVE TABLE sensors_bronze_dlt_fd (
  CONSTRAINT correct_schema EXPECT (_rescued_data IS NULL)
)
COMMENT "raw user data coming from json files ingested in incremental with Auto Loader to support schema inference and evolution"
AS SELECT * FROM cloud_files("/mnt/databricks-souzan-field-demo/turbine/incoming-data-json", "json")

-- COMMAND ----------

CREATE LIVE TABLE turbine_power_bronze_dlt_fd
(
  date string,
  power double, 
  theoretical_power_curve double, 
  turbine_id bigint, 
  wind_direction double, 
  wind_speed double
  )
  COMMENT "raw turbine power data coming from json files"
AS SELECT * FROM json.`/mnt/databricks-souzan-field-demo/turbine/power/raw`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2/ Silver layer: clean bronze sensors, power data and read in status

-- COMMAND ----------

CREATE INCREMENTAL LIVE TABLE sensors_silver_dlt_fd (
  CONSTRAINT valid_id EXPECT (id IS NOT NULL and id > 0) 
)
COMMENT "User data cleaned and anonymized for analysis."
AS SELECT AN3,
            AN4,
            AN5,
            AN6,
            AN7,
            AN8,
            AN9,
            ID,
            SPEED,
            TIMESTAMP
from STREAM(live.sensors_bronze_dlt_fd)

-- COMMAND ----------

CREATE INCREMENTAL LIVE TABLE status_silver_dlt_fd (
  CONSTRAINT valid_id EXPECT (id IS NOT NULL and id > 0)
)
COMMENT "Turbine status"
AS SELECT * FROM cloud_files('/mnt/databricks-souzan-field-demo/turbine/status', "parquet", map("schema", "id int, status string"))

-- COMMAND ----------

CREATE LIVE TABLE turbine_power_silver_dlt_fd (
  CONSTRAINT valid_id EXPECT (turbine_id IS NOT NULL and turbine_id > 0)
  )
  COMMENT "cleaned turbine power data."
AS SELECT
  cast(turbine_id as int),
  to_timestamp(date) as date,
  unix_timestamp(to_timestamp(date)) as timestamp,
  cast(power as double),
  cast(theoretical_power_curve as double),
  cast(wind_direction as double),
  cast(wind_speed as double)
FROM LIVE.turbine_power_bronze_dlt_fd

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3/ Gold layer: Join turbine sensors & status for ML and calculate average power

-- COMMAND ----------

CREATE INCREMENTAL LIVE TABLE sensor_gold_dlt_fd (
  CONSTRAINT valid_id EXPECT (id IS NOT NULL and id > 0) ON VIOLATION DROP ROW
)
COMMENT "Final sensor table with all information for Analysis / ML"
AS SELECT * FROM STREAM(live.sensors_silver_dlt_fd) LEFT JOIN live.status_silver_dlt_fd USING (id)

-- COMMAND ----------

CREATE LIVE TABLE turbine_power_gold_dlt_fd (
  CONSTRAINT valid_id EXPECT (turbine_id IS NOT NULL and turbine_id > 0)
  )
  COMMENT "gold turbine power data ready for analysis"
AS SELECT
  *,
  avg(power) over (PARTITION BY turbine_id ORDER BY timestamp range between 7200 PRECEDING and CURRENT ROW) AS average_power
FROM LIVE.turbine_power_silver_dlt_fd

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC How that our pipeline is declared let's run it using Delta Live Tables
-- MAGIC 
-- MAGIC 
-- MAGIC <img src="https://raw.githubusercontent.com/souzanmirza-db/databricks-demo/main/iot-wind-turbine/resources/images/dlt_image.png" width="90%" />
-- MAGIC 
-- MAGIC [Checkout the DLT Pipeline](https://e2-demo-field-eng.cloud.databricks.com/?o=1444828305810485#joblist/pipelines/c0a42e33-daea-46c0-85b8-0f5ef65940ff/updates/11857ec8-33e8-4f53-9839-08b097372391)
