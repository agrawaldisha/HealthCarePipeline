#!/usr/bin/env python
# coding: utf-8

# In[1]:


from google.cloud import storage, bigquery
from pyspark.sql import SparkSession
import datetime
import json

# Initialize Spark Session (if not already)
try:
    spark
except NameError:
    spark = SparkSession.builder         .appName("MyApp")         .getOrCreate()

# GCS and BigQuery Clients
storage_client = storage.Client()
bq_client = bigquery.Client()

# Configuration
GCS_BUCKET = "healthcare-bucket-192"
HOSPITAL_NAME = "hospital-a"
LANDING_PATH = f"gs://{GCS_BUCKET}/landing/{HOSPITAL_NAME}/"
ARCHIVE_PATH = f"gs://{GCS_BUCKET}/landing/{HOSPITAL_NAME}/archive/"
CONFIG_FILE_PATH = f"gs://{GCS_BUCKET}/configs/load_config.csv"

BQ_PROJECT = "gcpdataengineering-467713"
BQ_AUDIT_TABLE = f"{BQ_PROJECT}.temp_dataset.audit_log"
BQ_LOG_TABLE = f"{BQ_PROJECT}.temp_dataset.pipeline_logs"

MYSQL_CONFIG = {
    "url": "jdbc:mysql://34.9.188.225:3306/hospital_a_db?useSSL=false&allowPublicKeyRetrieval=true",
    "driver": "com.mysql.cj.jdbc.Driver",
    "user": "myuser",
    "password": "Dishu_192"
}

# Logging
log_entries = []

def log_event(event_type, message, table=None):
    entry = {
        "timestamp": datetime.datetime.now().isoformat(),
        "event_type": event_type,
        "message": message,
        "table": table
    }
    log_entries.append(entry)
    print(f"[{entry['timestamp']}] {event_type} - {message}")

def save_logs_to_gcs():
    log_file = f"temp/pipeline_logs/pipeline_log_{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}.json"
    blob = storage_client.bucket(GCS_BUCKET).blob(log_file)
    blob.upload_from_string(json.dumps(log_entries, indent=4), content_type="application/json")
    print(f"✅ Logs saved to GCS: gs://{GCS_BUCKET}/{log_file}")

def save_logs_to_bigquery():
    if log_entries:
        log_df = spark.createDataFrame(log_entries)
        log_df.write.format("bigquery")             .option("table", BQ_LOG_TABLE)             .option("temporaryGcsBucket", GCS_BUCKET)             .mode("append")             .save()
        print("✅ Logs written to BigQuery")

def move_existing_files_to_archive(table):
    blobs = storage_client.bucket(GCS_BUCKET).list_blobs(prefix=f"landing/{HOSPITAL_NAME}/{table}/")
    for blob in blobs:
        if blob.name.endswith(".json"):
            file = blob.name
            date_part = file.split("_")[-1].split(".")[0]
            year, month, day = date_part[-4:], date_part[2:4], date_part[:2]
            archive_blob_name = f"landing/{HOSPITAL_NAME}/archive/{table}/{year}/{month}/{day}/{file.split('/')[-1]}"
            dest_blob = storage_client.bucket(GCS_BUCKET).blob(archive_blob_name)
            dest_blob.rewrite(blob)
            blob.delete()
            log_event("INFO", f"Moved {file} to {archive_blob_name}", table=table)

def get_latest_watermark(table_name):
    query = f"""
        SELECT MAX(load_timestamp) AS latest_timestamp
        FROM `{BQ_AUDIT_TABLE}`
        WHERE tablename = '{table_name}' AND data_source = "hospital_a_db"
    """
    try:
        result = bq_client.query(query).result()
        for row in result:
            return row.latest_timestamp or "1900-01-01 00:00:00"
    except Exception as e:
        log_event("ERROR", f"Failed to fetch watermark: {e}", table=table_name)
        return "1900-01-01 00:00:00"

def extract_and_save_to_landing(table, load_type, watermark_col):
    try:
        last_watermark = get_latest_watermark(table) if load_type.lower() == "incremental" else None
        log_event("INFO", f"Latest watermark for {table}: {last_watermark}", table=table)

        query = f"(SELECT * FROM {table}) AS t" if load_type.lower() == "full" else                 f"(SELECT * FROM {table} WHERE {watermark_col} > '{last_watermark}') AS t"

        df = (spark.read.format("jdbc")
              .option("url", MYSQL_CONFIG["url"])
              .option("user", MYSQL_CONFIG["user"])
              .option("password", MYSQL_CONFIG["password"])
              .option("driver", MYSQL_CONFIG["driver"])
              .option("dbtable", query)
              .load())

        count = df.count()
        log_event("SUCCESS", f"Extracted {count} rows from {table}", table=table)

        today = datetime.datetime.today().strftime('%d%m%Y')
        json_path = f"landing/{HOSPITAL_NAME}/{table}/{table}_{today}.json"
        json_data = df.toPandas().to_json(orient="records", lines=True) if count > 0 else ""
        storage_client.bucket(GCS_BUCKET).blob(json_path).upload_from_string(json_data, content_type="application/json")
        log_event("SUCCESS", f"Data written to GCS at {json_path}", table=table)

        audit_df = spark.createDataFrame([
            ("hospital_a_db", table, load_type, count, datetime.datetime.now(), "SUCCESS")
        ], ["data_source", "tablename", "load_type", "record_count", "load_timestamp", "status"])

        audit_df.write.format("bigquery")             .option("table", BQ_AUDIT_TABLE)             .option("temporaryGcsBucket", GCS_BUCKET)             .mode("append")             .save()
        log_event("SUCCESS", f"Audit logged for {table}", table=table)

    except Exception as e:
        log_event("ERROR", f"Extraction failed for {table}: {e}", table=table)

def read_config_file():
    try:
        df = spark.read.csv(CONFIG_FILE_PATH, header=True)
        log_event("INFO", "Config file read from GCS")
        return df
    except Exception as e:
        log_event("ERROR", f"Failed to read config: {e}")
        return None

# Execution
config_df = read_config_file()
if config_df:
    for row in config_df.collect():
        # Use attribute access for Spark Rows
        if row.is_active == '1' and row.datasource == "hospital_a_db":
            # Unpack with attribute access
            _, _, table, load_type, watermark_col, _, _ = row
            move_existing_files_to_archive(table)
            extract_and_save_to_landing(table, load_type, watermark_col)

# Final logs
save_logs_to_gcs()
save_logs_to_bigquery()

# Gracefully stop Spark only at the very end.
try:
    spark.stop()
except Exception as e:
    print(f"Warning: Exception while stopping Spark - {e}")


# In[ ]:


# Challanges :- If the table do not exists ?
# sometimes connections to the db fails due to tech glitch , change in configs I failed beacuse of the authorized networks
# Spark Session is killed silently
# used gs:// bymistake             .option("temporaryGcsBucket", GCS_BUCKET) \

#Logs generated 


[2025-08-04T14:39:56.948102] INFO - ✅ Successfully read the config file
                                                                                
[2025-08-04T14:40:00.411421] INFO - No existing files for table encounters
[2025-08-04T14:40:01.400284] INFO - Latest watermark for encounters: 1900-01-01 00:00:00
[2025-08-04T14:40:02.261349] SUCCESS - ✅ Successfully extracted data from encounters
                                                                                
[2025-08-04T14:40:05.597426] SUCCESS - ✅ JSON file successfully written to gs://healthcare-bucket-192/landing/hospital-a/encounters/encounters_04082025.json
                                                                                
[2025-08-04T14:40:20.730979] SUCCESS - ✅ Audit log updated for encounters
[2025-08-04T14:40:20.758998] INFO - No existing files for table patients
[2025-08-04T14:40:21.753406] INFO - Latest watermark for patients: 1900-01-01 00:00:00
[2025-08-04T14:40:22.008675] SUCCESS - ✅ Successfully extracted data from patients
[2025-08-04T14:40:23.162369] SUCCESS - ✅ JSON file successfully written to gs://healthcare-bucket-192/landing/hospital-a/patients/patients_04082025.json
                                                                                
[2025-08-04T14:40:35.353333] SUCCESS - ✅ Audit log updated for patients
[2025-08-04T14:40:35.385913] INFO - No existing files for table transactions
[2025-08-04T14:40:36.515180] INFO - Latest watermark for transactions: 1900-01-01 00:00:00
[2025-08-04T14:40:36.781461] SUCCESS - ✅ Successfully extracted data from transactions
                                                                                
[2025-08-04T14:40:39.571988] SUCCESS - ✅ JSON file successfully written to gs://healthcare-bucket-192/landing/hospital-a/transactions/transactions_04082025.json
                                                                                
[2025-08-04T14:40:47.979005] SUCCESS - ✅ Audit log updated for transactions
[2025-08-04T14:40:48.002546] INFO - No existing files for table providers
[2025-08-04T14:40:48.002598] INFO - Latest watermark for providers: None
[2025-08-04T14:40:48.249196] SUCCESS - ✅ Successfully extracted data from providers
[2025-08-04T14:40:48.808063] SUCCESS - ✅ JSON file successfully written to gs://healthcare-bucket-192/landing/hospital-a/providers/providers_04082025.json
                                                                                
[2025-08-04T14:40:57.795551] SUCCESS - ✅ Audit log updated for providers
[2025-08-04T14:40:57.817477] INFO - No existing files for table departments
[2025-08-04T14:40:57.817525] INFO - Latest watermark for departments: None
[2025-08-04T14:40:58.062150] SUCCESS - ✅ Successfully extracted data from departments
[2025-08-04T14:40:58.584539] SUCCESS - ✅ JSON file successfully written to gs://healthcare-bucket-192/landing/hospital-a/departments/departments_04082025.json
                                                                                
[2025-08-04T14:41:07.508641] SUCCESS - ✅ Audit log updated for departments
✅ Logs successfully saved to GCS at gs://healthcare-bucket-192/temp/pipeline_logs/pipeline_log_20250804144107.json
                                                                                
✅ Logs stored in BigQuery for future analysis




# rerun logs 


[2025-08-04T15:04:01.825755] INFO - Config file read from GCS
[2025-08-04T15:04:03.130861] INFO - Moved landing/hospital-a/encounters/encounters_04082025.json to landing/hospital-a/archive/encounters/2025/08/04/encounters_04082025.json
[2025-08-04T15:04:04.166542] INFO - Latest watermark for encounters: 2025-08-04 14:47:13.904624+00:00
                                                                                
[2025-08-04T15:04:07.034496] SUCCESS - Extracted 0 rows from encounters
[2025-08-04T15:04:07.190495] SUCCESS - Data written to GCS at landing/hospital-a/encounters/encounters_04082025.json
                                                                                
[2025-08-04T15:04:25.396609] SUCCESS - Audit logged for encounters
[2025-08-04T15:04:26.616070] INFO - Latest watermark for patients: 2025-08-04 14:47:31.606692+00:00
[2025-08-04T15:04:27.341033] SUCCESS - Extracted 0 rows from patients
[2025-08-04T15:04:27.496143] SUCCESS - Data written to GCS at landing/hospital-a/patients/patients_04082025.json
                                                                                
[2025-08-04T15:04:39.803861] SUCCESS - Audit logged for patients
[2025-08-04T15:04:41.057745] INFO - Latest watermark for transactions: 2025-08-04 14:40:40.429157+00:00
                                                                                
[2025-08-04T15:04:42.777374] SUCCESS - Extracted 0 rows from transactions
[2025-08-04T15:04:42.942221] SUCCESS - Data written to GCS at landing/hospital-a/transactions/transactions_04082025.json
                                                                                
[2025-08-04T15:04:49.489982] SUCCESS - Audit logged for transactions
[2025-08-04T15:04:49.513605] INFO - Latest watermark for providers: None
[2025-08-04T15:04:50.344639] SUCCESS - Extracted 26 rows from providers
[2025-08-04T15:04:51.025068] SUCCESS - Data written to GCS at landing/hospital-a/providers/providers_04082025.json
                                                                                
[2025-08-04T15:04:59.714515] SUCCESS - Audit logged for providers
[2025-08-04T15:04:59.741160] INFO - Latest watermark for departments: None
[2025-08-04T15:05:00.389652] SUCCESS - Extracted 21 rows from departments
[2025-08-04T15:05:00.967031] SUCCESS - Data written to GCS at landing/hospital-a/departments/departments_04082025.json
                                                                                
[2025-08-04T15:05:07.460345] SUCCESS - Audit logged for departments
✅ Logs saved to GCS: gs://healthcare-bucket-192/temp/pipeline_logs/pipeline_log_20250804150507.json
                                                                                
✅ Logs written to BigQuery


# In[ ]:




