# Databricks notebook source
# MAGIC %md
# MAGIC ### Model Training and ML
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC <img src="https://s3.us-east-2.amazonaws.com/databricks-knowledge-repo-images/ML/ML-workflow.png" width=1000>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ## Training and Deploying a Model to Predict the Risk of Heart Disease

# COMMAND ----------

# MAGIC %md
# MAGIC #### Brief Over View
# MAGIC 1) Data Aquisition: Used a realistic simulation of patient EMR data, using [synthea](https://github.com/synthetichealth/synthea), for 10,000 patients in Massachusetts. Note that all data used in this demo is simulated data for fake individuals.
# MAGIC 
# MAGIC 2) Ingestion: load the data from flatfiles (csv) stored on the github repo and ingest the data into spark DataFrames
# MAGIC   
# MAGIC 3) Exploration: Demonstrate the platform's capabilities for query, multi language support and visualization of the data
# MAGIC   
# MAGIC 4) Training, tracking and saving a model using MLFlow: We show how to create a classifer to predict if a patient is diabetic given demographic information and obesety 

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Data Ingest

# COMMAND ----------

# MAGIC %md
# MAGIC Create path for your files on dbfs

# COMMAND ----------

dbutils.fs.mkdirs("dbfs:/FileSore/NIH")

# COMMAND ----------

# MAGIC %md
# MAGIC Download files

# COMMAND ----------

# MAGIC %sh
# MAGIC wget -O /dbfs/FileSore/NIH/conditions.csv https://raw.githubusercontent.com/azureinfra/NIH/master/Databricks/conditions.csv
# MAGIC wget -O /dbfs/FileSore/NIH/patients.csv https://raw.githubusercontent.com/azureinfra/NIH/master/Databricks/patients.csv

# COMMAND ----------

# MAGIC %md
# MAGIC Make sure that all files are downloaded

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/FileSore/NIH"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ingest EHR data into Spark DataFrames

# COMMAND ----------

ehr_dfs = {}
for path,name in [(m[0],m[1]) for m in dbutils.fs.ls("dbfs:/FileSore/NIH/")]:
  df_name = name.replace('.csv','')
  ehr_dfs[df_name] = spark.read.csv(path,header=True,inferSchema=True)

# COMMAND ----------

out_str="<b>There are {} tables in this collection with:</b><br>".format(len(ehr_dfs))
for k in ehr_dfs:
  out_str+="<b>{}</b> records in <b>{}</b> dataset<br>".format(ehr_dfs[k].count(),k)
  
displayHTML(out_str)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Exploration, insights
# MAGIC Let's see if there is a correlation between being smoker and heart disease

# COMMAND ----------

display(ehr_dfs['conditions'])

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Creating a dataframe for assessing the link between heart attack and smoking

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

display(ehr_dfs['conditions'])

# COMMAND ----------

smoking_heartDisease_df = (
  ehr_dfs['conditions']
  .select('PATIENT',F.lower(F.col('DESCRIPTION')).alias('DESC'))
  .withColumn('has_heart_disease',F.lower(F.col('DESC')).like('%coronary heart disease%'))
  .withColumn('is_smoker', F.lower(F.col('DESC')).like('%smokes tobacco%'))
  .drop('DESC')
  .groupBy('PATIENT')
  .agg(F.max('has_heart_disease').alias('has_heart_disease'),F.max('is_smoker').alias('is_smoker'))
  .select('PATIENT',F.col('has_heart_disease').cast('int'),F.col('is_smoker').cast('int'))
)
display(smoking_heartDisease_df)

# COMMAND ----------

# MAGIC %md
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC <img src="https://upload.wikimedia.org/wikipedia/commons/thumb/1/1b/R_logo.svg/724px-R_logo.svg.png" width=100>
# MAGIC </div>
# MAGIC Use R's statistical tools to investigate correlation between diabetes and obesety

# COMMAND ----------

# MAGIC %md
# MAGIC first let's register our dataframe as a temporary view where we can access it via other languages (R, SQL etc)

# COMMAND ----------

smoking_heartDisease_df.createOrReplaceTempView('DfForR')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from DfForR

# COMMAND ----------

# MAGIC %md
# MAGIC Use chi-squred test to verify association between conditions

# COMMAND ----------

# MAGIC %r
# MAGIC library(SparkR,stats)
# MAGIC df <- sql("SELECT has_heart_disease, is_smoker FROM DfForR")
# MAGIC df1=collect(df)
# MAGIC chiTest=chisq.test(table(df1))
# MAGIC print(chiTest)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Now let's create a dataset for training a model that predicts whether a person is diabetic based on obesity and demographics

# COMMAND ----------

patients_df = (
  ehr_dfs['patients']
 .select('id','MARITAL','RACE','ETHNICITY','GENDER','BIRTHPLACE','ZIP', F.to_date('BIRTHDATE').alias('BIRTHDATE'),F.to_date('DEATHDATE').alias('DEATHDATE'))
 .withColumn('is_dead',F.when(F.isnull('DEATHDATE'),0).otherwise(1))
 .withColumn('age', F.when(F.col('is_dead')==1, F.year('DEATHDATE')-F.year('BIRTHDATE')).otherwise(F.year(F.current_date()) - F.year('BIRTHDATE')))
 .withColumnRenamed('id','pid')
 .select('pid','MARITAL','RACE','ETHNICITY','GENDER','ZIP', 'age')
)

# COMMAND ----------

patients_df = (
  patients_df
  .withColumn('MARITAL',F.coalesce(F.col('MARITAL'),F.lit('null')))
  .withColumn('ZIP',F.coalesce(F.col('ZIP'),F.lit('null')))
  .withColumn('RACE',F.coalesce(F.col('RACE'),F.lit('null')))
  .withColumn('ETHNICITY',F.coalesce(F.col('ETHNICITY'),F.lit('null')))
  .withColumn('GENDER',F.coalesce(F.col('GENDER'),F.lit('null')))
)

# COMMAND ----------

dataset = patients_df.join(smoking_heartDisease_df.withColumnRenamed('PATIENT','pid'),on='pid').drop('pid')

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Use MLFlow to track/tune and load a model

# COMMAND ----------

# MAGIC %md
# MAGIC #### Note: Make sure to attach mlflow package to your cluster
# MAGIC Define a function to wrap building feature eng stages

# COMMAND ----------

from pyspark.ml import Pipeline
from pyspark.ml.feature import OneHotEncoderEstimator, StringIndexer, VectorAssembler, Normalizer
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.classification import LogisticRegression

import mlflow
import mlflow.spark

def get_feature_eng_stages(categoricalColumns, label="has_heart_disease"):

  stages = [] # stages in our Pipeline
  for categoricalCol in categoricalColumns:
      # Category Indexing with StringIndexer
      stringIndexer = StringIndexer(inputCol=categoricalCol, outputCol=categoricalCol + "Index")
      # Use OneHotEncoder to convert categorical variables into binary SparseVectors
      encoder = OneHotEncoderEstimator(inputCols=[stringIndexer.getOutputCol()], outputCols=[categoricalCol + "classVec"])
      # Add stages.  These are not run here, but will run all at once later on.
      stages += [stringIndexer, encoder]
      
  label_stringIdx = StringIndexer(inputCol = label, outputCol="label")
  stages += [label_stringIdx]
  numericCols = ["age", "is_smoker"]
  assemblerInputs = [c + "classVec" for c in categoricalColumns] + numericCols
  assembler = VectorAssembler(inputCols=assemblerInputs, outputCol="raw_features")
  normalizer = Normalizer(inputCol="raw_features", outputCol="features", p=1.0)
  stages += [assembler,normalizer]
  
  return(stages)
  

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Prepare the dataset for training

# COMMAND ----------

categoricalColumns = ['MARITAL','RACE','ETHNICITY','GENDER','ZIP']  
stages = get_feature_eng_stages(categoricalColumns)
partialPipeline = Pipeline().setStages(stages)
pipelineModel = partialPipeline.fit(dataset)
preppedDataDF = pipelineModel.transform(dataset)

# COMMAND ----------

cols = dataset.columns
selectedcols = ["label", "features"] + cols
dataset = preppedDataDF.select(selectedcols)
display(dataset)

# COMMAND ----------

selectedcols = ["label", "features"] + cols
dataset = preppedDataDF.select(selectedcols)
display(dataset)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Train a classifier to predict disease status and log/track parameters and metrics using [MLFlow](https://www.mlflow.org/)

# COMMAND ----------

# MAGIC %md
# MAGIC Run training

# COMMAND ----------

run_info = train_tarck_and_load(dataset, max_iter=20,reg_param=0.1)

# COMMAND ----------

run_info.to_proto()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##To see the MLFlow dashbord for this experiment, click on the <img src="https://docs.azuredatabricks.net/_images/mlflow-runs-link-icon.png" width=100></div> icon on the notebook

# COMMAND ----------

# MAGIC %md
# MAGIC Now let's load the model that we just used trained to predict heart disease risk on new patients

# COMMAND ----------

model = mlflow.spark.load_model(path="lrModel", run_id=run_info.run_uuid) #Use one of the run IDs we captured above

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Let's look at the prediction error by age group

# COMMAND ----------

df=model.transform(dataset)

# COMMAND ----------

df_pd=df.select((F.col('age')/10).cast('int').alias('age_group'),F.abs(F.col('prediction')-F.col('has_heart_disease')).alias('error')).toPandas()

# COMMAND ----------

import seaborn as sns
sns.set(style="whitegrid")

g=sns.violinplot(x='age_group', y='error', data=df_pd, kind='boxen')
display(g.figure)

# COMMAND ----------


