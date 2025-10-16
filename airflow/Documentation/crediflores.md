# ETL\_CREDIFLORES DAG Documentation

## Overview

`ETL_CREDIFLORES` is an Apache Airflow DAG designed to orchestrate the end-to-end ETL process for the Crediflores dataset. It handles reading parameters, extracting files from SFTP, uploading them to GCS, processing data into BigQuery and MySQL, executing dbt transformations, syncing contact information to Respond.io, and sending email notifications with logs.

---

## DAG Configuration

* **DAG ID**: `ETL_CREDIFLORES`
* **Schedule**: `0 10 * * 1-5` (Runs at 10:00 AM, Monday to Friday)
* **Timezone**: `America/Bogota`
* **Start Date**: `2025-02-24`
* **Retries**: 0
* **Tags**: `['PythonDataFlow']`

---

## TaskGroups & Tasks

### 1. Initialization

* `reset_logger`: Clears existing logs using `CustomLogger`.

### 2. Cleanup

* `delete_bigquery_tables`: Deletes existing tables from BigQuery.
* `clear_specific_gcs_files`: Deletes specific files from GCS.
* `vaciar_carpeta`: Empties a local extraction folder.

### 3. Parameter Handling

* `read_params_to_json`: Reads an Excel file and saves parameters as JSON.
* `obtener_parametros_proceso`: Filters parameter JSON by prefix (e.g., `PCS`, `PCGCS`, etc.).
* `usar_parametros`: Converts dicts into class-like objects.

### 4. SFTP Tasks

* `sftp`: Downloads the latest file from SFTP, validates and transforms the data, and saves it as a CSV.

### 5. GCS Upload

* `upload_to_gcs`: Uploads the latest extracted file to two folders in GCS: `raw/` and `backup/`.

### 6. BigQuery Load

* `load_data_to_bigquery`: Loads CSV from GCS into BigQuery. It supports `TRUNCATE`, `DROP`, or `INSERT_INTO` operations, infers schema, and includes error handling with fallback.

### 7. dbt Layers

* `bronze_layer`: Runs `dbt run` for `base`, then `dbt snapshot` for `base_snapshot`.
* `silver_layer`: Runs `dbt run` for `cred_inc`.
* `gold_layer`: Runs `dbt run` for `mart_base` and `mart_respond`.

### 8. MySQL Transfer

* `truncate_mysql_table`: Clears the MySQL destination table.
* `load_to_mysql`: Reads data from BigQuery and loads it into MySQL after type casting.

### 9. Contact Synchronization

* `fetch_contacts`: Extracts contacts from BigQuery.
* `send_all_to_respondio`: Sends formatted contact information to the Respond.io API.

### 10. Email Notification

* `send_email_notification`: Sends success or failure notification emails, includes logs and a custom HTML table from `CustomLogger`.

---

## External Resources

### Google Cloud

* **BigQuery**

  * Project: `charlieserver-281513`
  * Dataset: `crediflores_prod`
  * Tables: `mart_base`, `mart_respond`, and others

* **Cloud Storage**

  * Bucket: `produbanco`
  * Folders: `raw/`, `backup/`

### MySQL

* Host: `34.74.221.96`
* Database: `crediflores`
* Table: `cred_clients`

---

## Credentials

* All services use a shared GCP service account: `/opt/airflow/gcp/credencial_charlie.json`

---

## Utilities

* **CustomLogger**: Used across tasks for consistent logging and event emission
* **Excel to JSON**: Parameters are loaded from `params_crediflores.xlsx` and saved as `params_crediflores.json`
* **Date Pattern Replacement**: Dynamic file naming based on date tokens like `YYYYMMDD`

---

## Error Handling

* Task-specific exception handling with `AirflowFailException` and `try/except` blocks.
* Email alerts sent on failure with inline logs.
* GCS and BigQuery loads are tolerant to missing files.

---

## Dependencies

* Python libraries: `pandas`, `paramiko`, `google-cloud-storage`, `google-cloud-bigquery`, `requests`
* Airflow providers: `docker`, `email`, `python`, `task_group`
* External APIs: Respond.io
* dbt profiles and configurations mounted inside Docker container for transformation tasks

---

## Execution Flow

```text
reset_logger
  └── cleanup_tasks
         ├── delete_bigquery_tables
         ├── clear_specific_gcs_files
         └── vaciar_carpeta
  └── read_params_to_json
        └── sftp_tasks
        └── gcs_tasks
        └── bq_tasks
              └── bronze_layer
              └── silver_layer
              └── gold_layer
                    └── my_sql_tasks
                          └── sync_contact
                                └── mail_task
```

---

## Author

Developed by: **Juan Felipe Toro**
