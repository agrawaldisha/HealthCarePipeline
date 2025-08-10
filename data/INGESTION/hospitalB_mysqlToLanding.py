#!/usr/bin/env python
# coding: utf-8

# In[1]:


from google.cloud import storage, bigquery
from pyspark.sql import SparkSession
import datetime
import json

# Initialize GCS & BigQuery Clients
storage_client = storage.Client()
bq_client = bigquery.Client()

# Initialize Spark Session
spark = SparkSession.builder.appName("HospitalBMySQLToLanding").getOrCreate()

# Google Cloud Storage (GCS) Configuration
GCS_BUCKET = "healthcare-bucket-192"
HOSPITAL_NAME = "hospital-b"
LANDING_PATH = f"gs://{GCS_BUCKET}/landing/{HOSPITAL_NAME}/"
ARCHIVE_PATH = f"gs://{GCS_BUCKET}/landing/{HOSPITAL_NAME}/archive/"
CONFIG_FILE_PATH = f"gs://{GCS_BUCKET}/configs/load_config.csv"

# BigQuery Configuration
BQ_PROJECT = "gcpdataengineering-467713"
BQ_AUDIT_TABLE = f"{BQ_PROJECT}.temp_dataset.audit_log"
BQ_LOG_TABLE = f"{BQ_PROJECT}.temp_dataset.pipeline_logs"

# MySQL Configuration
MYSQL_CONFIG = {
    "url": "jdbc:mysql://34.45.40.110:3306/hospital_b_db?useSSL=false&allowPublicKeyRetrieval=true",
    "driver": "com.mysql.cj.jdbc.Driver",
    "user": "myuser",
    "password": "Dishu_192"
}

# Logging Mechanism
log_entries = []

def log_event(event_type, message, table=None):
    log_entry = {
        "timestamp": datetime.datetime.now().isoformat(),
        "event_type": event_type,
        "message": message,
        "table": table
    }
    log_entries.append(log_entry)
    print(f"[{log_entry['timestamp']}] {event_type} - {message}")

def save_logs_to_gcs():
    log_filename = f"pipeline_log_{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}.json"
    log_filepath = f"temp/pipeline_logs/{log_filename}"
    json_data = json.dumps(log_entries, indent=4)
    bucket = storage_client.bucket(GCS_BUCKET)
    blob = bucket.blob(log_filepath)
    blob.upload_from_string(json_data, content_type="application/json")
    print(f"✅ Logs successfully saved to GCS at gs://{GCS_BUCKET}/{log_filepath}")

def save_logs_to_bigquery():
    if log_entries:
        log_df = spark.createDataFrame(log_entries)
        log_df.write.format("bigquery")             .option("table", BQ_LOG_TABLE)             .option("temporaryGcsBucket", GCS_BUCKET)             .mode("append")             .save()
        print("✅ Logs stored in BigQuery for future analysis")

# Move Existing Files to Archive
def move_existing_files_to_archive(table):
    blobs = list(storage_client.bucket(GCS_BUCKET).list_blobs(prefix=f"landing/{HOSPITAL_NAME}/{table}/"))
    existing_files = [blob.name for blob in blobs if blob.name.endswith(".json")]

    if not existing_files:
        log_event("INFO", f"No existing files for table {table}")
        return

    for file in existing_files:
        source_blob = storage_client.bucket(GCS_BUCKET).blob(file)
        date_part = file.split("_")[-1].split(".")[0]
        year, month, day = date_part[-4:], date_part[2:4], date_part[:2]
        archive_path = f"landing/{HOSPITAL_NAME}/archive/{table}/{year}/{month}/{day}/{file.split('/')[-1]}"
        destination_blob = storage_client.bucket(GCS_BUCKET).blob(archive_path)
        storage_client.bucket(GCS_BUCKET).copy_blob(source_blob, storage_client.bucket(GCS_BUCKET), destination_blob.name)
        source_blob.delete()
        log_event("INFO", f"Moved {file} to {archive_path}", table=table)

# Get Latest Watermark from BigQuery
def get_latest_watermark(table_name):
    query = f"""
        SELECT MAX(load_timestamp) AS latest_timestamp
        FROM `{BQ_AUDIT_TABLE}`
        WHERE tablename = '{table_name}' AND data_source = "hospital_b_db"
    """
    try:
        query_job = bq_client.query(query)
        result = query_job.result()
        for row in result:
            return row.latest_timestamp if row.latest_timestamp else "1900-01-01 00:00:00"
        return "1900-01-01 00:00:00"
    except Exception as e:
        log_event("ERROR", f"Failed to fetch watermark: {e}", table=table_name)
        return "1900-01-01 00:00:00"

# Extract Data and Save to Landing
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
        log_event("SUCCESS", f"✅ Successfully extracted {count} rows from {table}", table=table)

        today = datetime.datetime.today().strftime('%d%m%Y')
        JSON_FILE_PATH = f"landing/{HOSPITAL_NAME}/{table}/{table}_{today}.json"

        data_to_write = df.toPandas().to_json(orient="records", lines=True) if count > 0 else "[]"
        bucket = storage_client.bucket(GCS_BUCKET)
        blob = bucket.blob(JSON_FILE_PATH)
        blob.upload_from_string(data_to_write, content_type="application/json")

        log_event("SUCCESS", f"✅ JSON file successfully written to gs://{GCS_BUCKET}/{JSON_FILE_PATH}", table=table)

        audit_df = spark.createDataFrame([
            ("hospital_b_db", table, load_type, count, datetime.datetime.now(), "SUCCESS")
            ], ["data_source", "tablename", "load_type", "record_count", "load_timestamp", "status"])

        (audit_df.write.format("bigquery")
            .option("table", BQ_AUDIT_TABLE)
            .option("temporaryGcsBucket", GCS_BUCKET)  # Use bucket name only here
            .mode("append")
            .save())

        log_event("SUCCESS", f"✅ Audit log updated for {table}", table=table)

    except Exception as e:
        log_event("ERROR", f"Error processing {table}: {str(e)}", table=table)

# Read Config File from GCS
def read_config_file():
    df = spark.read.csv(CONFIG_FILE_PATH, header=True)
    log_event("INFO", "✅ Successfully read the config file")
    return df

# Process active config entries
config_df = read_config_file()

for row in config_df.collect():
    # Use attribute access for Spark Row
    if row.is_active == '1' and row.datasource == "hospital_b_db":
        _, _, table, load_type, watermark, _, targetpath = row
        move_existing_files_to_archive(table)
        extract_and_save_to_landing(table, load_type, watermark)

save_logs_to_gcs()
save_logs_to_bigquery()


# In[ ]:


# Logs generated 

[2025-08-04T15:11:48.152787] INFO - ✅ Successfully read the config file
[2025-08-04T15:11:48.838584] INFO - No existing files for table encounters
[2025-08-04T15:11:50.139686] INFO - Latest watermark for encounters: 1900-01-01 00:00:00
                                                                                
[2025-08-04T15:11:52.889031] SUCCESS - ✅ Successfully extracted 10000 rows from encounters
                                                                                
[2025-08-04T15:11:55.049401] SUCCESS - ✅ JSON file successfully written to gs://healthcare-bucket-192/landing/hospital-b/encounters/encounters_04082025.json
                                                                                
[2025-08-04T15:12:06.974515] SUCCESS - ✅ Audit log updated for encounters
[2025-08-04T15:12:07.002889] INFO - No existing files for table patients
[2025-08-04T15:12:08.380698] INFO - Latest watermark for patients: 1900-01-01 00:00:00
[2025-08-04T15:12:09.125831] SUCCESS - ✅ Successfully extracted 5000 rows from patients
                                                                                
[2025-08-04T15:12:13.605065] SUCCESS - ✅ JSON file successfully written to gs://healthcare-bucket-192/landing/hospital-b/patients/patients_04082025.json
                                                                                
[2025-08-04T15:12:28.095830] SUCCESS - ✅ Audit log updated for patients
[2025-08-04T15:12:28.119794] INFO - No existing files for table transactions
[2025-08-04T15:12:29.291666] INFO - Latest watermark for transactions: 1900-01-01 00:00:00
[2025-08-04T15:12:30.457336] SUCCESS - ✅ Successfully extracted 10000 rows from transactions
                                                                                
[2025-08-04T15:12:32.710742] SUCCESS - ✅ JSON file successfully written to gs://healthcare-bucket-192/landing/hospital-b/transactions/transactions_04082025.json
                                                                                
[2025-08-04T15:12:40.595761] SUCCESS - ✅ Audit log updated for transactions
[2025-08-04T15:12:40.630778] INFO - No existing files for table providers
[2025-08-04T15:12:40.630830] INFO - Latest watermark for providers: None
[2025-08-04T15:12:41.293562] SUCCESS - ✅ Successfully extracted 31 rows from providers
[2025-08-04T15:12:41.841912] SUCCESS - ✅ JSON file successfully written to gs://healthcare-bucket-192/landing/hospital-b/providers/providers_04082025.json
                                                                                
[2025-08-04T15:12:48.473862] SUCCESS - ✅ Audit log updated for providers
[2025-08-04T15:12:48.506266] INFO - No existing files for table departments
[2025-08-04T15:12:48.506320] INFO - Latest watermark for departments: None
[2025-08-04T15:12:49.137031] SUCCESS - ✅ Successfully extracted 21 rows from departments
[2025-08-04T15:12:49.655093] SUCCESS - ✅ JSON file successfully written to gs://healthcare-bucket-192/landing/hospital-b/departments/departments_04082025.json
                                                                                
[2025-08-04T15:12:56.224259] SUCCESS - ✅ Audit log updated for departments
✅ Logs successfully saved to GCS at gs://healthcare-bucket-192/temp/pipeline_logs/pipeline_log_20250804151256.json
                                                                                
✅ Logs stored in BigQuery for future analysis
ated 



# rerun pipeline 

[2025-08-04T15:13:20.897998] INFO - ✅ Successfully read the config file
[2025-08-04T15:13:21.489577] INFO - Moved landing/hospital-b/encounters/encounters_04082025.json to landing/hospital-b/archive/encounters/2025/08/04/encounters_04082025.json
[2025-08-04T15:13:22.582461] INFO - Latest watermark for encounters: 2025-08-04 15:11:55.049516+00:00
[2025-08-04T15:13:23.218737] SUCCESS - ✅ Successfully extracted 0 rows from encounters
[2025-08-04T15:13:23.371087] SUCCESS - ✅ JSON file successfully written to gs://healthcare-bucket-192/landing/hospital-b/encounters/encounters_04082025.json
                                                                                
[2025-08-04T15:13:29.554275] SUCCESS - ✅ Audit log updated for encounters
[2025-08-04T15:13:29.877953] INFO - Moved landing/hospital-b/patients/patients_04082025.json to landing/hospital-b/archive/patients/2025/08/04/patients_04082025.json
[2025-08-04T15:13:30.932962] INFO - Latest watermark for patients: 2025-08-04 15:12:13.605168+00:00
[2025-08-04T15:13:31.572472] SUCCESS - ✅ Successfully extracted 0 rows from patients
[2025-08-04T15:13:31.719408] SUCCESS - ✅ JSON file successfully written to gs://healthcare-bucket-192/landing/hospital-b/patients/patients_04082025.json
                                                                                
[2025-08-04T15:13:38.958320] SUCCESS - ✅ Audit log updated for patients
[2025-08-04T15:13:39.302652] INFO - Moved landing/hospital-b/transactions/transactions_04082025.json to landing/hospital-b/archive/transactions/2025/08/04/transactions_04082025.json
[2025-08-04T15:13:40.302845] INFO - Latest watermark for transactions: 2025-08-04 15:12:32.710894+00:00
[2025-08-04T15:13:40.957555] SUCCESS - ✅ Successfully extracted 0 rows from transactions
[2025-08-04T15:13:41.116075] SUCCESS - ✅ JSON file successfully written to gs://healthcare-bucket-192/landing/hospital-b/transactions/transactions_04082025.json
                                                                                
[2025-08-04T15:13:47.155266] SUCCESS - ✅ Audit log updated for transactions
[2025-08-04T15:13:47.490597] INFO - Moved landing/hospital-b/providers/providers_04082025.json to landing/hospital-b/archive/providers/2025/08/04/providers_04082025.json
[2025-08-04T15:13:47.490744] INFO - Latest watermark for providers: None
[2025-08-04T15:13:48.096253] SUCCESS - ✅ Successfully extracted 31 rows from providers
[2025-08-04T15:13:48.545863] SUCCESS - ✅ JSON file successfully written to gs://healthcare-bucket-192/landing/hospital-b/providers/providers_04082025.json
                                                                                
[2025-08-04T15:13:54.444700] SUCCESS - ✅ Audit log updated for providers
[2025-08-04T15:13:54.777831] INFO - Moved landing/hospital-b/departments/departments_04082025.json to landing/hospital-b/archive/departments/2025/08/04/departments_04082025.json
[2025-08-04T15:13:54.777981] INFO - Latest watermark for departments: None
[2025-08-04T15:13:55.378175] SUCCESS - ✅ Successfully extracted 21 rows from departments
[2025-08-04T15:13:55.841849] SUCCESS - ✅ JSON file successfully written to gs://healthcare-bucket-192/landing/hospital-b/departments/departments_04082025.json
                                                                                
[2025-08-04T15:14:04.913603] SUCCESS - ✅ Audit log updated for departments
✅ Logs successfully saved to GCS at gs://healthcare-bucket-192/temp/pipeline_logs/pipeline_log_20250804151404.json
                                                                                
✅ Logs stored in BigQuery for future analysis

