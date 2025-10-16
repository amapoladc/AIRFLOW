{{ config(
    materialized         = 'incremental',
    incremental_strategy = 'merge',
    unique_key           = 'tarjeta_dig_str',
    on_schema_change     = 'sync'
) }}        -- agrega columnas nuevas si aparecen

WITH clean AS (
  SELECT
    CAST(cedula      AS STRING) AS cedula_str,
    CAST(tarjeta_dig AS STRING) AS tarjeta_dig_str,
    nombre, apellido,
  FROM {{ source('raw_data','DATA_WAREHAUSE_CLEAN') }}
),

cobranza AS (
  SELECT
    CAST(CEDULA      AS STRING) AS cedula_str,
    CAST(TARJETA_DIG AS STRING) AS tarjeta_dig_str,
    NOMBRE_COMPLETO,
  FROM {{ source('raw_data','DATA_WAREHAUSE_BL_COBRANZA') }}
),

joined AS (
  SELECT
      COALESCE(cobranza.cedula_str , clean.cedula_str)       AS cedula,
      COALESCE(cobranza.tarjeta_dig_str, clean.tarjeta_dig_str)
                                                            AS tarjeta_dig_str,
      COALESCE(cobranza.NOMBRE_COMPLETO,
               CONCAT(clean.nombre, ' ', clean.apellido))    AS nombre_completo,

      CURRENT_TIMESTAMP()                                   AS last_update_ts
  FROM clean
  FULL OUTER JOIN cobranza
    ON clean.tarjeta_dig_str = cobranza.tarjeta_dig_str
)

SELECT *
FROM joined
{% if is_incremental() %}
WHERE last_update_ts >= (
        SELECT COALESCE(MAX(last_update_ts), TIMESTAMP '1970-01-01')
        FROM {{ this }}
)
{% endif %}
