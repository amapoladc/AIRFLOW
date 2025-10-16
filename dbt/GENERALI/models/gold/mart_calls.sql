{{ config(
    materialized='incremental',
    unique_key='unique_id'
) }}

WITH SilverCalls AS (
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
  FROM {{ ref('call_cleaning') }} -- Make sure this refs the corrected model above

  {% if is_incremental() %}
    -- Solo trae los nuevos registros (basado en fecha_carga o timestamp)
    -- Consider using a timestamp column if fecha_carga might not be strictly ordered
    WHERE CAST(fecha_carga AS TIMESTAMP) > (SELECT MAX(CAST(fecha_carga AS TIMESTAMP)) FROM {{ this }}) -- Example: Cast if needed for comparison
    -- Or if fecha_carga is DATE: WHERE fecha_carga > (SELECT MAX(fecha_carga) FROM {{ this }})
  {% endif %}
)

SELECT * FROM SilverCalls