{{ config(materialized='table') }}

WITH src AS (
  SELECT
    -- Normalizo número de entrada como STRING
    CASE 
      WHEN CAST(phone_number_x AS STRING) = '1128895750' THEN '551128895750'
      ELSE CAST(phone_number_x AS STRING)
    END AS numero_entrada_llamada,

    -- Identificador del operador ('0' => 'não atribuído')
  CASE 
    WHEN COALESCE(NULLIF(TRIM(CAST(no__agent AS STRING)), ''), '0') = '0'
      THEN 'não atribuído'
    ELSE COALESCE(NULLIF(TRIM(CAST(no__agent AS STRING)), ''), '0')
  END AS identificador_operador,


    -- Tiempos crudos (pueden venir como STRING/DATETIME/TIMESTAMP)
    start_time AS start_time_raw,
    end_time   AS end_time_raw,

    -- Parseo seguro de wait_duration 'HH:MM:SS' a TIME
    CASE
      WHEN wait_duration IS NOT NULL
           AND REGEXP_CONTAINS(wait_duration, r'^\d{1,2}:\d{2}:\d{2}$')
        THEN SAFE.PARSE_TIME('%H:%M:%S', wait_duration)
      ELSE NULL
    END AS wait_time,

    call_duration,                           -- STRING
    call_status,
    CAST(direct_inward_dialing AS STRING) AS direct_inward_dialing,
    CAST(uniqueid AS STRING)              AS unique_id,
    CAST(portfolio AS STRING)             AS portfolio,
    CAST(attention_type AS STRING)        AS attention_type,
    CAST(state AS STRING)                 AS state,
    CAST(reason AS STRING)                AS reason,
    CAST(call_source AS STRING)           AS call_source,
    CAST(call_queue AS STRING)            AS call_queue,
    CAST(call_type AS STRING)             AS call_type,
    CAST(call_transfer AS STRING)         AS call_transfer,
    CAST(phone_number_y AS STRING)        AS phone_number_y,
    CAST(call_status_detail AS STRING)    AS call_status_detail,

    CAST(date___time AS TIMESTAMP)        AS datetime_ts,
    CAST(duration_seg_ AS INT64)          AS duration_seconds,
    CAST(cedula_ruc AS STRING)            AS id_number,
    CAST(first_name AS STRING)            AS first_name,
    CAST(last_name AS STRING)             AS last_name,
    CAST(fecha_carga AS TIMESTAMP)        AS fecha_carga
  FROM {{ source('raw_data', 'DATA_WAREHAUSE_FINAL_MERGE') }}
),

-- Normalizo start_time/end_time a TIMESTAMP
norm AS (
  SELECT
    s.*,

    -- Intento 1: ya es TIMESTAMP
    -- Intento 2: es DATETIME -> interpreto en 'America/Bogota'
    -- Intento 3/4: es STRING en formatos comunes
    COALESCE(
      SAFE_CAST(start_time_raw AS TIMESTAMP),
      TIMESTAMP(SAFE_CAST(start_time_raw AS DATETIME), 'America/Sao_Paulo'),
      SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', CAST(start_time_raw AS STRING)),
      SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', CAST(start_time_raw AS STRING))
    ) AS start_ts,

    COALESCE(
      SAFE_CAST(end_time_raw AS TIMESTAMP),
      TIMESTAMP(SAFE_CAST(end_time_raw AS DATETIME), 'America/Sao_Paulo'),
      SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', CAST(end_time_raw AS STRING)),
      SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', CAST(end_time_raw AS STRING))
    ) AS end_ts
  FROM src s
),

calc AS (
  SELECT
    numero_entrada_llamada,
    identificador_operador,

    CASE
      WHEN wait_time IS NULL THEN NULL
      ELSE EXTRACT(HOUR   FROM wait_time) * 3600
         + EXTRACT(MINUTE FROM wait_time) * 60
         + EXTRACT(SECOND FROM wait_time)
    END AS wait_seconds,

    start_ts,
    end_ts,
    wait_time,

    call_duration,
    call_status,
    direct_inward_dialing,
    unique_id,
    portfolio,
    attention_type,
    state,
    reason,
    call_source,
    call_queue,
    call_type,
    call_transfer,
    phone_number_y,
    call_status_detail,
    datetime_ts,
    duration_seconds,
    id_number,
    first_name,
    last_name,
    fecha_carga
  FROM norm
)

SELECT
  numero_entrada_llamada,
  identificador_operador,

  -- Para abandonadas: start_time = end_ts - wait_seconds (si ambos existen)
  CASE
    WHEN call_status = 'Abandoned'
         AND end_ts IS NOT NULL
         AND wait_seconds IS NOT NULL
      THEN TIMESTAMP_SUB(end_ts, INTERVAL wait_seconds SECOND)
    ELSE start_ts
  END AS start_time,

  end_ts AS end_time,

  -- Conservo el TIME original y los segundos
  wait_time       AS wait_duration,
  wait_seconds,

  call_duration,
  call_status,
  direct_inward_dialing,
  unique_id,
  portfolio,
  attention_type,
  state,
  reason,
  call_source,
  call_queue,
  call_type,
  call_transfer,
  phone_number_y,
  call_status_detail,
  datetime_ts AS datetime,
  duration_seconds,
  id_number,
  first_name,
  last_name,
  fecha_carga
FROM calc
