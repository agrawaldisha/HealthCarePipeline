#!/usr/bin/env python
# coding: utf-8

# In[4]:


from pyspark.sql import SparkSession, functions as f
from pyspark.sql.functions import when

# Create Spark session
spark = SparkSession.builder     .appName("Claims Ingestion")     .getOrCreate()

# Define source and sink variables
BUCKET_NAME = "healthcare-bucket-192"
CPT_CODES_BUCKET_PATH = f"gs://{BUCKET_NAME}/landing/cptcodes/*.csv"
BQ_TABLE = "gcpdataengineering-467713.bronze_dataset.cpt_codes"
TEMP_GCS_BUCKET = BUCKET_NAME

# Read CSV files from GCS
cpt_codes_df = spark.read.csv(CPT_CODES_BUCKET_PATH, header=True)

#remove space from column name
cptcolumns=cpt_codes_df.columns

#replace space with underscores

for col in cptcolumns:
    new_col=col.replace(" ","_").lower()
    cpt_codes_df=cpt_codes_df.withColumnRenamed(col,new_col)


cpt_codes_df.write     .format("bigquery")     .option("table", BQ_TABLE)     .option("temporaryGcsBucket", TEMP_GCS_BUCKET)     .mode("overwrite")     .save()


# In[ ]:


# Py4JJavaError: An error occurred while calling o108.save.
# : java.lang.RuntimeException: Failed to write to BigQuery
        
        
# - ..likely comes from invalid column names, especially column names with spaces, which BigQuery does not allow.

