#!/usr/bin/env python
# coding: utf-8

# In[21]:


from pyspark.sql import SparkSession, functions as f
from pyspark.sql.functions import when

# Create Spark session
spark = SparkSession.builder     .appName("Claims Ingestion")     .getOrCreate()

# Define source and sink variables
BUCKET_NAME = "healthcare-bucket-192"
CLAIMS_BUCKET_PATH = f"gs://{BUCKET_NAME}/landing/claims/*.csv"
BQ_TABLE = "gcpdataengineering-467713.bronze_dataset.claims"
TEMP_GCS_BUCKET = BUCKET_NAME

# Read CSV files from GCS
claims_df = spark.read.csv(CLAIMS_BUCKET_PATH, header=True)

# Add datasource column based on file path
claims_df = claims_df.withColumn(
        "datasource", 
        when(f.input_file_name().contains("hospital2"), "hospb")
        .when(f.input_file_name().contains("hospital1"), "hospa")
        .otherwise("none")
    )
claims_df=claims_df.dropDuplicates()
# Write to BigQuery
claims_df.write     .format("bigquery")     .option("table", BQ_TABLE)     .option("temporaryGcsBucket", TEMP_GCS_BUCKET)     .mode("overwrite")     .save()


# In[29]:


# Read files AND immediately extract file name
claims_df = spark.read.csv(CLAIMS_BUCKET_PATH, header=True)     .withColumn("file_name", f.input_file_name())

# Confirm file names
claims_df.select("file_name").show(truncate=False)

# Assign datasource based on file name
claims_df = claims_df.withColumn(
    "datasource", 
    when(f.col("file_name").contains("hospital2"), "hospb")
    .when(f.col("file_name").contains("hospital1"), "hospa")
    .otherwise("none")
)

# Drop duplicates
claims_df = claims_df.dropDuplicates()

# Optional: Count by datasource
claims_df.groupBy("datasource").count().show()

# Write to BigQuery
claims_df.write     .format("bigquery")     .option("table", BQ_TABLE)     .option("temporaryGcsBucket", TEMP_GCS_BUCKET)     .mode("overwrite")     .save()


# In[ ]:




