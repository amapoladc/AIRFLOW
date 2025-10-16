{{ config(materialized='table') }}

-- CTE: castea tipos y calcula duraciones en segundos
WITH ParsedCalls AS (
  SELECT
    numero_entrada_llamada,
    start_time,
    call_status,
    identificador_operador,
    direct_inward_dialing,   -- se castea luego con SAFE_CAST
    wait_duration,           -- TIME en el bronze
    call_duration,           -- STRING (HH:MM:SS)
    CAST(unique_id AS STRING)     AS unique_id,
    CAST(portfolio AS STRING)     AS portfolio,
    CAST(attention_type AS STRING) AS attention_type,
    CAST(state AS STRING)         AS state,
    CAST(reason AS STRING)        AS reason,
    CAST(call_source AS STRING)   AS call_source,
    CAST(fecha_carga AS STRING)   AS fecha_carga,

    -- ✅ De TIME a segundos usando EXTRACT (evita REGEXP_CONTAINS)
    CASE
      WHEN wait_duration IS NOT NULL THEN
        EXTRACT(HOUR   FROM wait_duration) * 3600 +
        EXTRACT(MINUTE FROM wait_duration) * 60  +
        EXTRACT(SECOND FROM wait_duration)
      ELSE 0
    END AS wait_seconds,

    -- call_duration sigue siendo STRING; aquí sí conviene validar patrón
    CASE
      WHEN call_duration IS NOT NULL
           AND REGEXP_CONTAINS(CAST(call_duration AS STRING), r'^\d{1,2}:\d{2}:\d{2}$')
        THEN SAFE_CAST(SPLIT(call_duration, ':')[OFFSET(0)] AS INT64) * 3600 +
             SAFE_CAST(SPLIT(call_duration, ':')[OFFSET(1)] AS INT64) * 60  +
             SAFE_CAST(SPLIT(call_duration, ':')[OFFSET(2)] AS INT64)
      ELSE 0
    END AS call_seconds
  FROM {{ ref('call_data') }}
),

-- CTE: calcula timestamps y resuelve posibles duplicados por unique_id
RankedCalls AS (
  SELECT
    CAST(numero_entrada_llamada AS STRING) AS numero_entrada_llamada,
    CAST(start_time AS TIMESTAMP)          AS hora_entrada_llamada,

    CAST(NULL AS INT64) AS hora_entrada_file_espera,

    CAST(
      CASE
        WHEN call_status != 'Abandoned'
          THEN TIMESTAMP_ADD(CAST(start_time AS TIMESTAMP), INTERVAL COALESCE(wait_seconds, 0) SECOND)
        ELSE NULL
      END AS TIMESTAMP
    ) AS hora_llamada_atendida_operador,

    CAST(
      TIMESTAMP_ADD(CAST(start_time AS TIMESTAMP),
                    INTERVAL (COALESCE(wait_seconds, 0) + COALESCE(call_seconds, 0)) SECOND)
      AS TIMESTAMP
    ) AS hora_llamada_desligada,

    CAST(
      CASE
        WHEN call_status = 'Abandoned'
          THEN TIMESTAMP_ADD(CAST(start_time AS TIMESTAMP),
                             INTERVAL (COALESCE(wait_seconds, 0) + COALESCE(call_seconds, 0)) SECOND)
        ELSE NULL
      END AS TIMESTAMP
    ) AS hora_llamada_abandonada,

    CAST(
      CASE WHEN call_status != 'Abandoned' THEN call_seconds ELSE NULL END
      AS INT64
    ) AS tiempo_de_llamada,

    CAST(wait_seconds AS INT64) AS tiempo_de_espera,

    CAST(NULL AS INT64) AS tiempo_en_mute,

    CAST(identificador_operador AS STRING) AS identificador_operador,
    CAST(call_status AS STRING)            AS call_status,
    SAFE_CAST(direct_inward_dialing AS INT64) AS direct_inward_dialing,
    CAST(unique_id AS STRING)              AS unique_id,
    CAST(portfolio AS STRING)              AS portfolio,
    CAST(attention_type AS STRING)         AS attention_type,
    CAST(state AS STRING)                  AS state,
    CAST(reason AS STRING)                 AS reason,
    CAST(call_source AS STRING)            AS call_source,
    CAST(fecha_carga AS STRING)            AS fecha_carga,

    ROW_NUMBER() OVER (
      PARTITION BY unique_id
      ORDER BY CAST(start_time AS TIMESTAMP) ASC
    ) AS rn
  FROM ParsedCalls
)

SELECT
  numero_entrada_llamada,
  hora_entrada_llamada,
  hora_entrada_file_espera,
  hora_llamada_atendida_operador,
  hora_llamada_desligada,
  hora_llamada_abandonada,
  tiempo_de_llamada,
  tiempo_de_espera,
  tiempo_en_mute,
  identificador_operador,
  call_status,
  direct_inward_dialing,
  unique_id,
  portfolio,
  attention_type,
  state,
  reason,
  call_source,
  fecha_carga
FROM RankedCalls
WHERE rn = 1
