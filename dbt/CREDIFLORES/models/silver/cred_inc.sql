{{ config(
  materialized='incremental',
  unique_key='UNIQUE_ID',
  incremental_strategy='merge',
  on_schema_change='ignore',
  post_hook=[
    "
    UPDATE {{ this }} AS t
    SET t.RESPOND = 0,
        t.CAMPANA = 0
    WHERE NOT EXISTS (
      SELECT 1
      FROM {{ ref('base') }} AS k
      WHERE CAST(t.CEDULA AS STRING)      = CAST(k.CEDULA AS STRING)
        AND CAST(t.TARJETA_DIG AS STRING) = CAST(k.TARJETA_DIG AS STRING)
    ) OR t.RESPOND= 0
    "
  ]
) }}

WITH source AS (
  SELECT * FROM {{ ref('base_snapshot') }}
),
prioritized AS (
  SELECT *,
         ROW_NUMBER() OVER (
           PARTITION BY UNIQUE_ID
           ORDER BY RESPOND DESC, TIME_STAMP DESC
         ) AS row_num
  FROM source
)

SELECT *
FROM prioritized
WHERE row_num = 1
{% if is_incremental() %}
  AND DATE(FECHA_PROCESO) >= COALESCE((
        SELECT DATE(MAX(FECHA_PROCESO))
        FROM {{ this }}
      ), DATE('1970-01-01'))
{% endif %}
