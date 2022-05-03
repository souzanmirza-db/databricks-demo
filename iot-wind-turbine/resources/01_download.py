# Databricks notebook source
# MAGIC %md 
# MAGIC This notebook is in progress

# COMMAND ----------

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

display(spark.read.parquet('/mnt/quentin-demo-resources/turbine/gold/'))

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from parquet.`/mnt/quentin-demo-resources/turbine/incoming-data`

# COMMAND ----------

# MAGIC %sh ls /tmp/wind_turbine_download/Damaged

# COMMAND ----------

# MAGIC %sh head /tmp/wind_turbine_download/Damaged/D1.mat

# COMMAND ----------

import os
from scipy.io import loadmat
mats = []
root_dir = '/tmp/wind_turbine_download/Damaged'
for root, dirs, files in os.walk(root_dir):
    for file in files:
        mats.append(loadmat(root_dir+'/'+file))

# COMMAND ----------

import numpy as np
combined_mat = {}
keys = ['Speed', 'Torque', 'AN3', 'AN4', 'AN5', 'AN6', 'AN7', 'AN8', 'AN9', 'AN10']
for k in keys:
  combined_mat[k] = np.concatenate([n[k] for n in mats]).flatten()
  print(k, combined_mat[k][0])

# COMMAND ----------

# MAGIC %pip install faker

# COMMAND ----------

from faker import Faker
fake = Faker()



# COMMAND ----------

help(fake.date_between)

# COMMAND ----------

k, combined_mat[k]

# COMMAND ----------

#check that there is enough entries for each sensor
from collections import defaultdict 
for k in keys:
  print(k, combined_mat[k].size)

#create the json data format
wind_turbine_data = []
for i in range(10): #combined_mat[k].size):
  json_data = {}
  for k in keys:
    json_data[k] = combined_mat[k][i] 
    json_data['TIMESTAMP'] = fake.date_time_between(start_date='now', end_date='-30d')
  wind_turbine_data.append({'key': i, 'value': json_data}) 
    

# COMMAND ----------

wind_turbine_data

# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------

schema = StructType([StructField("Speed", StringType(), True),
StructField("Torque", StringType(), True),
StructField("AN3", StringType(), True),
StructField("AN4", StringType(), True),
StructField("AN5", StringType(), True),
StructField("AN6", StringType(), True),
StructField("AN7", StringType(), True),
StructField("AN8", StringType(), True),
StructField("AN9", StringType(), True),
StructField("AN10", StringType(), True),
  ])
 

# COMMAND ----------

type(combined_mat)

# COMMAND ----------

df = spark.createDataFrame(data=combined_mat, schema = schema)

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
