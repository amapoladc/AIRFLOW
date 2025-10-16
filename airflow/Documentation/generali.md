# ETL\_GENERALI DAG Documentation

## Overview

This DAG orchestrates a full end-to-end ETL process for the Generali project using Airflow. It includes parameter ingestion, web scraping via Selenium, data processing, Google Cloud Storage uploads, BigQuery loading, dbt model execution, and email notifications.

---

## DAG Metadata

* **DAG ID**: `ETL_GENERALI`
* **Schedule**: Daily at 5:00 AM (UTC-3)
* **Timezone**: America/Sao\_Paulo
* **Start Date**: 2025-02-24
* **Tags**: `['PythonDataFlow']`

---

## TaskGroups & Descriptions

### 1. `reset_logger`

Clears the custom log table before execution begins.

### 2. `clear_tasks`

Cleans up resources before the pipeline starts:

* `clear_specific_gcs_files`: Deletes specific files from GCS bucket.
* `delete_bigquery_tables`: Drops specific BigQuery tables.
* `vaciar_carpeta`: Empties local folder to prevent data contamination.

### 3. `read_params_to_json`

Reads configuration parameters from Excel and converts them to JSON for downstream tasks.

### 4. `web_scraping_tasks`

Responsible for extracting data from the web using Selenium:

* `obtener_parametros_proceso` and `usar_parametros`: Dynamically parse scraping configs.
* `download_call_detail_report`: Extracts call data.
* `download_campain_report`: Extracts campaign reports.

### 5. `preprocesamiento_tasks`

Processes and merges scraped CSVs into a unified dataset:

* `merge_and_process_calls`: Merges call and campaign data into a final CSV.

### 6. `gcs_tasks`

Uploads processed data to GCS:

* `upload_to_gcs`: Uploads final file to `raw` and `backup` folders in the specified bucket.

### 7. `bq_tasks`

Loads data from GCS into BigQuery:

* `load_data_to_bigquery`: Dynamically infers schema and loads data into target table with options to `DROP`, `TRUNCATE`, or `INSERT INTO`.

### 8. dbt Layer Execution

Executes dbt models per layer:

* **Bronze Layer**: Executes `call_data` model.
* **Silver Layer**: Executes `call_cleaning` model.
* **Gold Layer**: Executes `mart_calls` model.

### 9. `mail_task_group`

Sends email notifications:

* `send_email_notification`: Sends summary to internal team.
* `send_client_notification`: Notifies clients of success or issues.

---

## Key Components

### Logger

Custom logging utility (`CustomLogger`) is used across all tasks to record metadata, status, and error details.

### Selenium

Used in both `download_call_detail_report` and `download_campain_report` to perform automated web scraping with error handling and debug screenshots.

### Credentials & Mounts

* Credentials managed via `/opt/airflow/gcp/dbt_test.json`.
* Mounted volumes include dbt project folders and credential paths for DockerOperator tasks.

---

## Notes

* GCS and BigQuery integrations rely on JSON key authentication.
* The entire DAG is dynamically parameterized to support scalability across different processes.
* Error handling includes screenshots and HTML capture for debugging web scraping tasks.
* Parallelism is achieved via `expand` to distribute scraping and processing workloads.

---

## Future Improvements

* Parameterize dbt model selection.
* Externalize file/folder paths to Airflow Variables.
* Use XComs or centralized metadata tracking for file validations.

---

## Author

* **Team**: Data Engineering
* **Company**: Memorial Technologies
* **Last Updated**: June 2025
