# Databricks notebook source
try:
    cloud_storage_path
except NameError:
    cloud_storage_path = dbutils.widgets.get("cloud_storage_path")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Wind Turbine Sensors Datasets
# MAGIC 
# MAGIC * The dataset used in this accelerator is from [NREL](https://www.nrel.gov/). National Renewable Energy Laboratory (NREL) in the US specializes in the research and development of renewable energy, energy efficiency, energy systems integration, and sustainable transportation. NREL is a federally funded research and development center sponsored by the Department of Energy and operated by the Alliance for Sustainable Energy, a joint venture between MRIGlobal and Battelle.
# MAGIC 
# MAGIC * NREL published this dataset on [OEDI](https://data.openei.org/submissions/738) in 2014. NREL collected data from a healthy and a damaged gearbox of the same design tested by the GRC. Vibration data were collected by accelerometers along with high-speed shaft RPM signals during the dynamometer testing.
# MAGIC   
# MAGIC * Further details about this dataset
# MAGIC   * Dataset title: Wind Turbine Gearbox Condition Monitoring Vibration Analysis Benchmarking Datasets
# MAGIC   * Dataset source URL: https://data.openei.org/submissions/738
# MAGIC   * Dataset license: please see dataset source URL above

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Download wind turbine sensor dataset

# COMMAND ----------

# MAGIC %sh
# MAGIC rm -rf /tmp/wind_turbine_download/
# MAGIC mkdir -p /tmp/wind_turbine_download/
# MAGIC curl -o /tmp/wind_turbine_download/Healthy.zip "https://data.openei.org/files/738/Healthy.zip" -s
# MAGIC curl -o /tmp/wind_turbine_download/Damaged.zip "https://data.openei.org/files/738/Damaged.zip" -s

# COMMAND ----------

ls /tmp/wind_turbine_download

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Unzip wind turbine sensors dataset
# MAGIC Breakdown of the data downloaded: https://www.kaggle.com/c/jigsaw-toxic-comment-classification-challenge/overview

# COMMAND ----------

# MAGIC %sh
# MAGIC cd /tmp/wind_turbine_download
# MAGIC unzip -o Damaged.zip
# MAGIC unzip -o Healthy.zip
# MAGIC ls .

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reformat Data
# MAGIC Reformatting the data from mat to dataframes

# COMMAND ----------

# MAGIC %mkdir /tmp/souzan/
# MAGIC %mkdir /tmp/souzan/turbine

# COMMAND ----------

# MAGIC %fs ls /mnt/quentin-demo-resources/turbine

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## Move Data to storage
# MAGIC 
# MAGIC Move the Jigsaw train and test data from the driver node to object storage so that it can be be ingested into Delta Lake.

# COMMAND ----------

for file in ['train','test','match','match_outcomes','player_ratings','players','chat','cluster_regions']:
  print(f"moving data from file:/tmp/toxicity_download/{file}.csv to {cloud_storage_path}/{file}/bronze.csv")
  dbutils.fs.mv(f"file:/tmp/toxicity_download/{file}.csv", f"{cloud_storage_path}/{file}/bronze.csv")
  print(file)

# COMMAND ----------

# MAGIC %sh ls /dbfs/mnt/field-demos/manufacturing/iot_turbine/

# COMMAND ----------

# MAGIC %md
# MAGIC Copyright Databricks, Inc. [2021]. The source in this notebook is provided subject to the [Databricks License](https://databricks.com/db-license-source).  All included or referenced third party libraries are subject to the licenses set forth below.
# MAGIC 
# MAGIC |Library Name|Library license | Library License URL | Library Source URL |
# MAGIC |---|---|---|---|
# MAGIC |Spark-nlp|Apache-2.0 License| https://github.com/JohnSnowLabs/spark-nlp/blob/master/LICENSE | https://github.com/JohnSnowLabs/spark-nlp/
# MAGIC |Kaggle|Apache-2.0 License |https://github.com/Kaggle/kaggle-api/blob/master/LICENSE|https://github.com/Kaggle/kaggle-api|
