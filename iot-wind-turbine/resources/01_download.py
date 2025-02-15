# Databricks notebook source
# MAGIC %pip install faker kaggle

# COMMAND ----------

try:
    cloud_storage_path = dbutils.widgets.get("cloud_storage_path")
except NameError:
    cloud_storage_path = '/'
print(cloud_storage_path)

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql.types import *
from pyspark.sql.functions import *
import os
from scipy.io import loadmat
import pyspark.pandas as pd
from faker import Faker
import datetime
os.environ['cloud_storage_path'] = f'/dbfs{cloud_storage_path}'

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

# DBTITLE 1,Download wind turbine sensor dataset
# MAGIC %sh
# MAGIC rm -rf /tmp/wind_turbine_download/
# MAGIC mkdir -p /tmp/wind_turbine_download/
# MAGIC curl -o /tmp/wind_turbine_download/Healthy.zip "https://data.openei.org/files/738/Healthy.zip" -s
# MAGIC curl -o /tmp/wind_turbine_download/Damaged.zip "https://data.openei.org/files/738/Damaged.zip" -s

# COMMAND ----------

# DBTITLE 1,Unzip wind turbine sensors dataset
# MAGIC %sh
# MAGIC cd /tmp/wind_turbine_download
# MAGIC unzip -o Damaged.zip
# MAGIC unzip -o Healthy.zip
# MAGIC ls .

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Raw Layer & Save
# MAGIC Reformatting the data from mat to dataframes

# COMMAND ----------

# MAGIC %md
# MAGIC #### Process the data

# COMMAND ----------

schema = StructType([
                    StructField('Speed', FloatType(), True),
                    StructField('Torque', FloatType(), True),
                    StructField('AN3', FloatType(), True),
                    StructField('AN4', FloatType(), True),
                    StructField('AN5', FloatType(), True),
                    StructField('AN6', FloatType(), True),
                    StructField('AN7', FloatType(), True),
                    StructField('AN8', FloatType(), True),
                    StructField('AN9', FloatType(), True),
                    StructField('AN10', FloatType(), True),
                    StructField('status', StringType(), True),
  ])
turbine_sensor_data = spark.createDataFrame([], schema)
turbine_sensor_data.printSchema()

# COMMAND ----------

damaged_root_dir = '/tmp/wind_turbine_download/Damaged'
healthy_root_dir = '/tmp/wind_turbine_download/Healthy'
keys = ['Speed', 'Torque', 'AN3', 'AN4', 'AN5', 'AN6', 'AN7', 'AN8', 'AN9', 'AN10']

for root, dirs, files in os.walk(damaged_root_dir):
    for file in files:
      mat_file = {k: v.flatten() for k, v in loadmat(damaged_root_dir+'/'+file).items() if k in keys}
      turbine_sensor_data = turbine_sensor_data.unionByName(pd.DataFrame(mat_file, columns=keys, dtype=float).to_spark().withColumn('status', lit('damaged')))

# COMMAND ----------

for root, dirs, files in os.walk(healthy_root_dir):
    for file in files:
      mat_file = {k: v.flatten() for k, v in loadmat(healthy_root_dir+'/'+file).items() if k in keys}
      turbine_sensor_data = turbine_sensor_data.unionByName(pd.DataFrame(mat_file, columns=keys, dtype=float).to_spark().withColumn('status', lit('healthy')))

# COMMAND ----------

turbine_sensor_data = turbine_sensor_data.withColumn('id', when(col('status')=='healthy', (rand(seed=1)*500).cast('int')).otherwise((rand(seed=1)*(1501-500)+500).cast('int'))).withColumn('key', monotonically_increasing_id())

# COMMAND ----------

fake = Faker()
def fake_time():
  return fake.date_time_between(start_date=datetime.date(2018,1,1), end_date=datetime.date(2018,12,31))
fake_time_udf = udf(fake_time, TimestampType())

turbine_sensor_data = turbine_sensor_data.withColumn('Timestamp', fake_time_udf())

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create Bronze Data & Move to Storage

# COMMAND ----------

cloud_storage_path

# COMMAND ----------

turbine_sensor_data = turbine_sensor_data.select(*[c.upper() for c in turbine_sensor_data.columns if c not in ['status', 'key']], 'key', 'status')
display(turbine_sensor_data)

# COMMAND ----------

# DBTITLE 1,incoming_data_json
 turbine_sensor_data.drop('key', 'status').write.mode('overwrite').json(f'{cloud_storage_path}/incoming-data-json')

# COMMAND ----------

# DBTITLE 1,incoming_data parquet
turbine_sensor_data.withColumn('value', to_json(struct(col("SPEED"), col("TORQUE"), col("AN3"), col("AN4"), col("AN5"), col("AN6"), col("AN7"), col("AN8"), col("AN9"), col("AN10"), col("ID"), col("TIMESTAMP")))).select("key", "value").write.mode('overwrite').parquet(f'{cloud_storage_path}/incoming-data')

# COMMAND ----------

# DBTITLE 1,status
turbine_sensor_data.select('id', 'status').distinct().write.mode('overwrite').parquet(f'{cloud_storage_path}/status')

# COMMAND ----------

# DBTITLE 1,Gold data for ML
turbine_sensor_data.select("AN3", "AN4", "AN5", "AN6", "AN7", "AN8", "AN9", "AN10", "SPEED", "status").write.mode('overwrite').format("delta").save(f'{cloud_storage_path}/gold-data-for-ml')

# COMMAND ----------

# MAGIC %md
# MAGIC Copyright Databricks, Inc. [2022]. The source in this notebook is provided subject to the [Databricks License](https://databricks.com/db-license-source).  All included or referenced third party data are subject to the licenses set forth below.
# MAGIC 
# MAGIC |Dataset Name|Dataset license | Dataset License URL | Dataset Source URL |
# MAGIC |---|---|---|---|
# MAGIC |Wind Turbine Gearbox Condition Monitoring Vibration Analysis Benchmarking Datasets|Creative Commons 4.0 License| https://data.openei.org/files/738/WindTurbineConditionMonitoringLicenseInfo.txt | https://data.openei.org/submissions/738
# MAGIC <!-- |Kaggle|Apache-2.0 License |https://github.com/Kaggle/kaggle-api/blob/master/LICENSE|https://github.com/Kaggle/kaggle-api| -->

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Wind Turbine Power
# MAGIC This dataset is from [Kaggle Scada Dataset](https://www.kaggle.com/datasets/berkerisen/wind-turbine-scada-dataset).
# MAGIC 
# MAGIC ### Installing Kaggle CLI to download the dataset
# MAGIC 
# MAGIC To use the Kaggle CLI, you need to create an acount on Kaggle (see [documentation here](https://www.kaggle.com/docs/api)). With this you will get a username and api_key.
# MAGIC 
# MAGIC To not expose the username and password on the notebook here, we use Databrick's secrets to store these informations and generate the json file required for running the Kaggle CLI. 
# MAGIC 1. Generate a secret scope and create two secrets using the databricks cli (one for username, one for password). See [documentation cli](https://docs.databricks.com/dev-tools/cli/index.html) and [documentation secrets](https://docs.databricks.com/security/secrets/index.html)
# MAGIC ```
# MAGIC databricks secrets create-scope --scope <scope-name>
# MAGIC databricks secrets put --scope <scope-name> --key <key-name>
# MAGIC ```
# MAGIC 2. The secrets in the notebook are, off course, redacted ([see here](https://docs.databricks.com/security/secrets/redaction.html)), so to use them we declare environment variables at the cluster creation ([see here](https://docs.databricks.com/security/secrets/secrets.html#store-the-path-to-a-secret-in-an-environment-variable)) and use an init script to create the json file. you can find the init.sh proposed in the ressource folder of this repos.
# MAGIC ```
# MAGIC if [ -n "$KAGGLE_USER" ]; then
# MAGIC   mkdir -p /root/.kaggle
# MAGIC   echo '{"username": ${KAGGLE_USER},"key": ${KAGGLE_APIKEY}}' > /root/.kaggle/kaggle.json
# MAGIC   chmod 600 /root/.kaggle/kaggle.json
# MAGIC   pip install kaggle
# MAGIC fi
# MAGIC ````
# MAGIC 
# MAGIC 3. The only thing left is to `pip install kaggle` to be able to run the cli
# MAGIC 4. You are now ready to use the Kaggle CLI with this cluster on any notebook you attach it to.

# COMMAND ----------

# MAGIC %run ./_kaggle_credential

# COMMAND ----------

if "KAGGLE_USERNAME" not in os.environ or os.environ['KAGGLE_USERNAME'] == "" or "KAGGLE_KEY" not in os.environ or os.environ['KAGGLE_KEY'] == "":
  print("You need to specify your KAGGLE USERNAME and KAGGLE KEY to download the data")
  print("Please open notebook under ./_resources/01_download and sepcify your Kaggle credential")
  raise RuntimeError("Kaggle credential is required to download the data. Please open notebook under ./_resources/kaggle_credential and sepcify your Kaggle credential")

# COMMAND ----------

# MAGIC %sh
# MAGIC rm -rf /dbfs/tmp/wind_turbine_download/
# MAGIC mkdir -p /dbfs/tmp/wind_turbine_download/
# MAGIC cd /dbfs/tmp/wind_turbine_download/
# MAGIC kaggle datasets download -d berkerisen/wind-turbine-scada-dataset 

# COMMAND ----------

# MAGIC %sh
# MAGIC cd /dbfs/tmp/wind_turbine_download
# MAGIC unzip -o wind-turbine-scada-dataset.zip
# MAGIC ls .

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Raw Data & Save

# COMMAND ----------

power = (spark.read.csv('/tmp/wind_turbine_download/T1.csv', header=True)
         .select(to_timestamp(col('Date/Time'), 'dd MM yyyy HH:mm').alias('date'), 
                 col('LV ActivePower (kW)').cast('float').alias('power'), 
                 col('Theoretical_Power_Curve (KWh)').cast('float').alias('theoretical_power_curve'), 
                  lit(1).cast('int').alias('turbine_id'),
                 col('Wind Direction (°)').cast('float').alias('wind_direction'), 
                 col('Wind Speed (m/s)').cast('float').alias('wind_speed')
                )
        )
display(power)

# COMMAND ----------

 power.write.mode('overwrite').json(f'{cloud_storage_path}/power/raw')
