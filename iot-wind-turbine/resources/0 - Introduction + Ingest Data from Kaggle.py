# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Image Segmentation Solutions Accelerator
# MAGIC 
# MAGIC In this solutions accelerator, we are providing a jump start to help you build an image segmentation on the Databricks Platform. 
# MAGIC 
# MAGIC The image segmentation problem has been facilitated in the recent years thanks to the advance in image based models and also the availability of pre-trained models that accelerate the training of your model for your specific dataset. This is called transfer learning.
# MAGIC 
# MAGIC ## Overall context
# MAGIC 
# MAGIC As part of an overall series on computer vision, Databricks decided to cover the image segmentation problems. In the context of manufacturing, these models can be very useful for quality teams (detecting area where damage occurs on electronics boards, wafer, mechanical part wear, wind turbine rotor damage, manufacturing defect, ...), for developing new services (environmental spill detection, forest fire evaluation, marine traffic monitoring, ...). 
# MAGIC 
# MAGIC To illustrate our point of view on this topic with our platform, we decided to use a famous Kaggle contest in the industry: [Airbus ship detection](https://www.kaggle.com/c/airbus-ship-detection) as a basis for this accelerator.
# MAGIC 
# MAGIC ## Structure of this accelerator
# MAGIC 
# MAGIC This accelerators will provide the end to end solution to managing image segmentation pipeline. Not only training the model, but also having a data pipeline to ingest data (yes - data do not arrive magically in a cloud storage 😅) and using the data for querying or BI (yes - when ML models are not made to show off in cocktail or use in a webapp, they are just a way to enrich data at scale 😉)
# MAGIC 
# MAGIC So the structure of the notebooks and workflow is as follows:
# MAGIC 0. this notebook - introduction and getting data from Kaggle
# MAGIC 1. building the data pipeline - ingesting as if your images were coming in real-time from your application and storing it in the Lakehouse so that you can query, use for ML, ...
# MAGIC 2. building the segmentation mode - covering the exploratory analysis, model training and hyperparameter tuning using parallelism and GPU
# MAGIC 3. using the model for serving - covering not only rest API serving but also batch prediction
# MAGIC 4. accessing the data as BI analysts for example.
# MAGIC 
# MAGIC ## Reference documents
# MAGIC 
# MAGIC - how to enable computer vision on the Lakehouse - [blog part 1](https://databricks.com/blog/2021/12/17/enabling-computer-vision-applications-with-the-data-lakehouse.html)
# MAGIC - Airbus Ship Detection Kaggle - [link here](https://www.kaggle.com/c/airbus-ship-detection)
# MAGIC - Stanford Paper on this problem - [link here](https://cs229.stanford.edu/proj2018/report/58.pdf)
# MAGIC - Image Segmentation on Medium - [link here](https://www.analyticsvidhya.com/blog/2019/04/introduction-image-segmentation-techniques-python/)
# MAGIC - State of the art solution for computer vision

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Step 0 - Setting up libraries and dataset

# COMMAND ----------

# DBTITLE 1,Creating a storage space for user
# Full username, e.g. "aaron.binns@databricks.com"
username = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')
# Short form of username, suitable for use as part of a topic name.
user = username.split("@")[0].replace(".","_")

# Creating the directory to store datas
storage_path = 'dbfs:/FileStore/Users/{}'.format(username)
dbutils.fs.mkdirs(storage_path)
# Creating the directory to store notebook resources (images)
resource_path = 'dbfs:/FileStore/Resources/satellite_boatdetection'
dbutils.fs.mkdirs(resource_path)

# Copying local resources to that repository
notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
path_list = ['file:/Workspace'] + notebook_path.split("/")[1:-1] + ['ressources/']
dbutils.fs.cp('/'.join(path_list), resource_path, True)

# COMMAND ----------

# MAGIC %md 
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

# DBTITLE 1,Download Dataset from Kagglem, unzip and save to local storage
# MAGIC %sh
# MAGIC kaggle competitions download -q -c airbus-ship-detection -p /local_disk0/
# MAGIC unzip -q /local_disk0/airbus-ship-detection.zip -d /local_disk0/airbus-ship-detection/

# COMMAND ----------

# DBTITLE 1,Move from local storage to Cloud Storage
dbutils.fs.cp('file:/local_disk0/airbus-ship-detection', storage_path+'/airbus-ship-detection/', True)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # 👉 Next Step
# MAGIC 
# MAGIC Now that the storage is setup - we can start to build the ingestion pipeline 
