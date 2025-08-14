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

def save_logs_to_bigquery():
    if log_entries:
        log_df = spark.createDataFrame(log_entries)
        log_df.write.format("bigquery")             .option("table", BQ_LOG_TABLE)             .option("temporaryGcsBucket", GCS_BUCKET)             .mode("append")             .save()

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

