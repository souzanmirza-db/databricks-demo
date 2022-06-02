# Databricks notebook source
# MAGIC %md
# MAGIC %md
# MAGIC # Downloading the data from Kaggle
# MAGIC 
# MAGIC ## Define KAGGLE_USERNAME and KAGGLE_KEY for authentication
# MAGIC 
# MAGIC * Instructions on how to obtain this information can be found [here](https://www.kaggle.com/docs/api).
# MAGIC 
# MAGIC * This information will need to be entered below. Please use [secret](https://docs.databricks.com/security/secrets/index.html) to avoid having your key as plain text

# COMMAND ----------

import os
os.environ['KAGGLE_USERNAME'] = "qambard" #dbutils.secrets.get(scope="kaggle", key="kaggle_username")
os.environ['KAGGLE_KEY'] = "95aa9b350d6a917bafbd65f65d8a9ab5" #dbutils.secrets.get(scope="kaggle", key="kaggle_key")

#Alternative option is to write your kaggle file in your driver using the cluster terminal option and load it:
#config = json.load(open(f"./config/{env}.conf", "r"))
#dbutils.widgets.text("config", json.dumps(config))
#os.environ['KAGGLE_USERNAME'] = config["kaggle_username"]
#os.environ['KAGGLE_KEY'] = config["kaggle_key"]
