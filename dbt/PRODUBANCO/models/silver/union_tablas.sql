-- models/bronze/union_tablas.sql

-- depends_on: {{ ref('cobranza') }}
-- depends_on: {{ ref('convenio') }}

{{ config(materialized='table') }}

-- We can explicitly reference the dbt `this` object’s database and schema, or the `target` object.
-- But typically with macros like `ref('something')`, dbt will handle fully qualified names 
-- behind the scenes, depending on how your project is configured.

{%- set cobranza_relation = adapter.get_relation(
    database='charlieserver-281513', 
    schema='produbanco_dev', 
    identifier='cobranza'
) -%}

{%- set convenio_relation = adapter.get_relation(
    database='charlieserver-281513', 
    schema='produbanco_dev', 
    identifier='convenio'
) -%}

{% if cobranza_relation and convenio_relation %}
    -- CASE 1: Both cobranza and convenio exist
    WITH tabla_1 AS (
        SELECT 
            NOMBRE_COMPLETO,
            CEDULA,
            DIRECCION,
            CELULAR,
            CORREO,
            CAST(CRD AS STRING) AS CRD,
            VALOR_TOTAL_DEUDA,
            SALDO_CAPITAL,
            PAGO_MINIMO,
            VALOR_NO_CUBIERTO,
            DIAS_MORA,
            TARJETA,
            MARCA,
            CALIFICACION,
            TARJETA_DIG,
            TIPO_OPERACION,
            FECHA_CORTE,
            BUCKET,
            ESTADO_PRD,
            EDAD,
            FECHA_PAGO,
            CAMPANIA,
            ESTADO_CONVENIO,
             CAST(TIME_STAMP AS DATE) AS TIME_STAMP,
            NOMBRE,
            APELLIDO,
            RESPOND,
            VALIDACION,
            CURRENT_DATE() AS FECHA_PROCESO
        FROM {{ ref('convenio') }}

        UNION ALL

        SELECT 
            NOMBRE_COMPLETO,
            CEDULA,
            DIRECCION,
            CELULAR,
            CORREO,
            NULL AS CRD,
            VALOR_TOTAL_DEUDA,
            SALDO_CAPITAL,
            PAGO_MINIMO,
            VALOR_NO_CUBIERTO,
            DIAS_MORA,
            TARJETA,
            MARCA,
            CALIFICACION,
            TARJETA_DIG,
            TIPO_OPERACION,
            FECHA_CORTE,
            BUCKET,
            ESTADO_PRD,
            EDAD,
            FECHA_PAGO,
            CAMPANIA,
            NULL AS ESTADO_CONVENIO,
             CAST(TIME_STAMP AS DATE) AS TIME_STAMP,
            NOMBRE,
            APELLIDO,
            RESPOND,
            VALIDACION,
            CURRENT_DATE() AS FECHA_PROCESO
        FROM {{ ref('cobranza') }}
    )
    SELECT *
    FROM tabla_1

{% elif cobranza_relation and not convenio_relation %}
    -- CASE 2: Only cobranza exists
    SELECT 
        NOMBRE_COMPLETO,
        CEDULA,
        DIRECCION,
        CELULAR,
        CORREO,
        NULL AS CRD,
        VALOR_TOTAL_DEUDA,
        SALDO_CAPITAL,
        PAGO_MINIMO,
        VALOR_NO_CUBIERTO,
        DIAS_MORA,
        TARJETA,
        MARCA,
        CALIFICACION,
        TARJETA_DIG,
        TIPO_OPERACION,
        FECHA_CORTE,
        BUCKET,
        ESTADO_PRD,
        EDAD,
        FECHA_PAGO,
        CAMPANIA,
        NULL AS ESTADO_CONVENIO,
         CAST(TIME_STAMP AS DATE) AS TIME_STAMP,
        NOMBRE,
        APELLIDO,
        RESPOND,
        VALIDACION,
        CURRENT_DATE() AS FECHA_PROCESO
    FROM {{ ref('cobranza') }}

{% elif convenio_relation and not cobranza_relation %}
    -- CASE 3: Only convenio exists
    SELECT 
        NOMBRE_COMPLETO,
        CEDULA,
        DIRECCION,
        CELULAR,
        CORREO,
        CAST(CRD AS STRING) AS CRD,
        VALOR_TOTAL_DEUDA,
        SALDO_CAPITAL,
        PAGO_MINIMO,
        VALOR_NO_CUBIERTO,
        DIAS_MORA,
        TARJETA,
        MARCA,
        CALIFICACION,
        TARJETA_DIG,
        TIPO_OPERACION,
        FECHA_CORTE,
        BUCKET,
        ESTADO_PRD,
        EDAD,
        FECHA_PAGO,
        CAMPANIA,
        ESTADO_CONVENIO,
         CAST(TIME_STAMP AS DATE) AS TIME_STAMP,
        NOMBRE,
        APELLIDO,
        RESPOND,
        VALIDACION,
        CURRENT_DATE() AS FECHA_PROCESO
    FROM {{ ref('convenio') }}

{% else %}
    -- If neither table exists (optional)
    SELECT 
        'No cobranza or convenio table found' AS message,
        CURRENT_DATE() AS fecha_proceso
{% endif %}
