-- models/staging/stg_bl_keys.sql
{{ config(
    enabled=true,
    materialized='table',
    cluster_by=['CEDULA','TARJETA_DIG'],
    tags=['staging','bl_keys']
) }}

WITH src AS (
  SELECT
    TRIM(CAST(CEDULA AS STRING))      AS CEDULA_RAW,
    TRIM(CAST(TARJETA_DIG AS STRING)) AS TARJETA_DIG_RAW
  FROM {{ source('raw_data','DATA_WAREHAUSE_BL_COBRANZA') }}
  WHERE CEDULA IS NOT NULL
    AND TARJETA_DIG IS NOT NULL
)
SELECT DISTINCT
  CEDULA_RAW AS CEDULA,
  TARJETA_DIG_RAW AS TARJETA_DIG
FROM src
