{{ config(
    materialized='incremental',
    unique_key='CEDULA',
    post_hook="
        UPDATE {{ this }} 
        SET ESTADO_CONVENIO = 0 
        WHERE CEDULA NOT IN (SELECT CEDULA FROM {{ ref('union_tablas') }});
    "
) }}
WITH source AS (
    SELECT * FROM {{ ref('union_snapshot') }}
),

-- Identificamos las CEDULAS que tienen más de un registro en el mismo TIME_STAMP
duplicates AS (
    SELECT 
        CEDULA, 
        TIME_STAMP
    FROM source
    GROUP BY CEDULA, TIME_STAMP
    HAVING COUNT(*) > 1
),

-- Aplicamos la lógica de prioridad para RESPOND y ajustamos VALIDACION
filtered AS (
    SELECT
        NOMBRE_COMPLETO,
        CEDULA,
        DIRECCION,
        CELULAR,
        CORREO,
        CRD,  
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
 
        CASE 
            WHEN VALIDACION = 'ok' THEN  2 -- Convertimos los números en STRING
            WHEN VALIDACION = 'Incontactable' THEN 3
            ELSE null
        END AS RESPOND,
        VALIDACION,
        FECHA_PROCESO,
        ROW_NUMBER() OVER (PARTITION BY CEDULA, TIME_STAMP ORDER BY RESPOND DESC) AS row_num
    FROM source
)

-- Seleccionamos la fila con la prioridad adecuada
SELECT 
    NOMBRE_COMPLETO,
    CEDULA,
    DIRECCION,
    CELULAR,
    CORREO,
    CRD,  
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
    FECHA_PROCESO
FROM filtered
WHERE row_num = 1

{% if is_incremental() %}
-- Solo insertar registros con un TIME_STAMP mayor al último registrado
AND TIME_STAMP > (SELECT MAX(TIME_STAMP) FROM {{ this }})
{% endif %}