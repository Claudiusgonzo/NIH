# Databricks notebook source
# MAGIC %md
# MAGIC ### Model Training and ML
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC <img src="https://s3.us-east-2.amazonaws.com/databricks-knowledge-repo-images/ML/ML-workflow.png" width=1000>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ## Training and Deploying a Model to Predict the Cost of a Patient's Next Visit

# COMMAND ----------

# MAGIC %md
# MAGIC #### Brief Over View
# MAGIC ######  1) Data Aquisition:
# MAGIC   Used a realistic simulation of patient EMR data, using [synthea](https://github.com/synthetichealth/synthea), for 20,000 patients in Massachusetts. Note that all data used in this demo is simulated data for fake individuals.
# MAGIC ###### 2) Ingestion
# MAGIC   load the data from flatfiles (csv) stored on the cloud and ingest the data into spark DataFrames
# MAGIC   
# MAGIC ###### 3) Exploration
# MAGIC   Demonstrate the platform's capabilities for query, multi language support and visualization of the data
# MAGIC   
# MAGIC ###### 4) Training, tracking and saving a model using MLFlow:
# MAGIC   We show how to create a classifer to predict the cost of the next encounter with a provider. 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import Libraries

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /FileStore/tables/patients.csv

# COMMAND ----------

from pyspark.sql import functions as F, Window

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ingest EHR data

# COMMAND ----------

display(dbutils.fs.ls("/data/hls/ehr/csv/"))

# COMMAND ----------

ehr_dfs = {}
for path,name in [(m[0],m[1]) for m in dbutils.fs.ls("/data/hls/ehr/csv/")]:
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
# MAGIC Let's see if there is a correlation between being diabetese and obesity

# COMMAND ----------

display(ehr_dfs['conditions'])

# COMMAND ----------

df = (
  ehr_dfs['conditions']
  .select('PATIENT',F.lower(F.col('DESCRIPTION')).alias('DESC'))
  .withColumn('has_diabetes',F.col('DESC').like('%diabetes%'))
  .withColumn('is_obese', F.col('DESC').like('%obese%'))
  .drop('DESC')
  .groupBy('PATIENT')
  .agg(F.max('has_diabetes').alias('is_diabetic'),F.max('is_obese').alias('is_obese'))
  .select(F.col('is_diabetic').cast('int'),F.col('is_obese').cast('int'))
)

# COMMAND ----------

# MAGIC %md
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC <img src="https://upload.wikimedia.org/wikipedia/commons/thumb/1/1b/R_logo.svg/724px-R_logo.svg.png" width=100>
# MAGIC </div>
# MAGIC Suppose we want to use R's statistical tools to investigate correlation between diabetes and obesety

# COMMAND ----------

df.createOrReplaceTempView('DfForR')

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %r
# MAGIC library(SparkR,stats)
# MAGIC df <- sql("SELECT * FROM DfForR")
# MAGIC df1=collect(df)
# MAGIC chiTest=chisq.test(table(df1))
# MAGIC print(chiTest)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create a dataset of encounters joined with patient information, procedure cost, and conditions

# COMMAND ----------

diabetes_bmi_df = (
  ehr_dfs['conditions']
  .select('PATIENT',F.lower(F.col('DESCRIPTION')).alias('DESC'))
  .withColumn('has_diabetes',F.col('DESC').like('%diabetes%'))
  .withColumn('is_obese', F.col('DESC').like('%obese%'))
  .drop('DESC')
  .groupBy('PATIENT')
  .agg(F.max('has_diabetes').alias('is_diabetic'),F.max('is_obese').alias('is_obese'))
  .select('PATIENT',F.col('is_diabetic').cast('float'),F.col('is_obese').cast('float'))
  .withColumnRenamed('PATIENT','pid')
)

# COMMAND ----------

display(diabetes_bmi_df)

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

dataset = patients_df.join(diabetes_bmi_df,on='pid').drop('pid')

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Use MLFlow to track/tune and load a model

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Function to wrap building feature eng stages and classification

# COMMAND ----------

from pyspark.ml import Pipeline
from pyspark.ml.feature import OneHotEncoderEstimator, StringIndexer, VectorAssembler
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.classification import LogisticRegression

import mlflow
import mlflow.spark

def get_feature_eng_stages(categoricalColumns, label="is_diabetic"):

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
  numericCols = ["age", "is_obese"]
  assemblerInputs = [c + "classVec" for c in categoricalColumns] + numericCols
  assembler = VectorAssembler(inputCols=assemblerInputs, outputCol="features")
  stages += [assembler]
  
  return(stages)
  

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
# MAGIC #here

# COMMAND ----------

def train_tarck_and_load(df, experiment_id, max_iter=10, reg_param=0.3):
  categoricalColumns = ['MARITAL','RACE','ETHNICITY','GENDER','ZIP']  
  
  with mlflow.start_run(experiment_id=experiment_id) as run:
    
    # Create initial LogisticRegression model
    (trainingData, testData) = dataset.randomSplit([0.7, 0.3], seed=100)
    lr = LogisticRegression(labelCol="label", featuresCol="features", maxIter=max_iter, regParam=reg_param)
    # Train model with Training Data
    pipeline = Pipeline(stages=[lr])
    
    lrModel = pipeline.fit(trainingData)
    predictions = lrModel.transform(testData)
    evaluator = BinaryClassificationEvaluator(rawPredictionCol="rawPrediction")
    areaUnderROC = evaluator.evaluate(predictions)
    
    mlflow.log_param("max_iter", max_iter)
    mlflow.log_param("reg_param", reg_param)
    mlflow.log_param("elastic_net_param", elastic_net_param)
    mlflow.log_metric("area_under_ROC", areaUnderROC)
    mlflow.spark.log_model(lrModel, "lrModel")
    return(run.info)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Train 

# COMMAND ----------

dbutils.widgets.removeAll()

dbutils.widgets.text("max_iter","10")
dbutils.widgets.text("reg_param","0.3")
dbutils.widgets.text("elastic_net_param","0.8")

dbutils.widgets.text("experiment_id","2540509")

max_iter = int(dbutils.widgets.get("max_iter"))
reg_param = float(dbutils.widgets.get("reg_param"))
elastic_net_param = float(dbutils.widgets.get("elastic_net_param"))
experiment_id = int(dbutils.widgets.get("experiment_id"))

# COMMAND ----------

run_info = train_tarck_and_load(dataset, experiment_id=experiment_id, max_iter=5,reg_param=0.1)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Now lets take a look at the [MLFlow dashbord](https://demo.cloud.databricks.com/#mlflow/experiments/2540509) for this experiment

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Load

# COMMAND ----------

model = mlflow.spark.load_model(path="lrModel", run_id=run_info.run_uuid) #Use one of the run IDs we captured above

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Use the model to predict cost from streaming data

# COMMAND ----------

display(model.transform(dataset))
