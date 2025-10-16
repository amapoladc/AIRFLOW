{{
    config(
        materialized='incremental',
        unique_key='ID_DEUDA',
        incremental_strategy='merge',
        partition_by={"field": "TIME_STAMP", "data_type": "DATE"}
    )
}}


WITH fact_deudas AS (
    SELECT
        GENERATE_UUID() AS ID_DEUDA,
        *
        
    

    FROM {{ ref('base') }}
)

SELECT *
FROM fact_deudas
{% if is_incremental() %}
WHERE TIME_STAMP > (
    SELECT MAX(TIME_STAMP)
    FROM {{ this }}
)
{% endif %}