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
        NOMBRE_COMPLETO,
        CEDULA,
        VALOR_TOTAL_DEUDA,
        SALDO_CAPITAL,
        PAGO_MINIMO,
        VALOR_NO_CUBIERTO,
        DIAS_MORA,
        MARCA,
        CALIFICACION,
        RESPOND,
        TARJETA_DIG,
        TIPO_OPERACION,
        FECHA_CORTE,
        BUCKET,
        ESTADO_PRD,
        EDAD,
        FECHA_PAGO,
        CAMPANIA,
        ESTADO_CONVENIO,
        TIME_STAMP,
        
    

    FROM {{ ref('union_tablas') }}
)

SELECT *
FROM fact_deudas
{% if is_incremental() %}
WHERE TIME_STAMP > (
    SELECT MAX(TIME_STAMP)
    FROM {{ this }}
)
{% endif %}