WITH source_data_cobranza AS (
    SELECT
        CAST(NOMBRE_COMPLETO AS STRING) AS NOMBRE_COMPLETO,
        {{ format_ID_EC('CEDULA') }} AS CEDULA,
        "example@email.com" AS CORREO,  -- Agrega un valor de ejemplo
        NULLIF(CAST(DIRECCION AS STRING), 'null') AS DIRECCION,
        CAST(CELULAR AS STRING) AS CELULAR,

        -- ✅ Use the parse_money macro to clean monetary values
        {{ parse_money('VALOR_TOTAL_DEUDA') }} AS VALOR_TOTAL_DEUDA,
        {{ parse_money('SALDO_CAPITAL') }} AS SALDO_CAPITAL,
        {{ parse_money('PAGO_MINIMO') }} AS PAGO_MINIMO,
        {{ parse_money('VALOR_NO_CUBIERTO') }} AS VALOR_NO_CUBIERTO,

        CAST(DIAS_MORA AS INT64) AS DIAS_MORA,
        CAST(TARJETA AS STRING) AS TARJETA,
        CAST(MARCA AS STRING) AS MARCA,
        CAST(CALIFICACION AS STRING) AS CALIFICACION,
        CAST(TARJETA_DIG AS STRING) AS TARJETA_DIG,
        CAST(TIPO_OPERACION AS STRING) AS TIPO_OPERACION,
        CAST(BUCKET AS STRING) AS BUCKET,
        CAST(ESTADO_PRD AS STRING) AS ESTADO_PRD,
        CAST(EDAD AS INT64) AS EDAD,
        CAST(VALIDACION AS STRING) AS VALIDACION,
        CAST(MES_CAMPANIA AS STRING) AS CAMPANIA,
        CAST(NOMBRE AS STRING) AS NOMBRE,
        CAST(APELLIDO AS STRING) AS APELLIDO,

        -- ✅ Use the parse_date_safe macro to handle date fields
        CAST(FECHA_CORTE as INT64) as FECHA_CORTE,
        {{ parse_date_safe('FECHA_PAGO') }} AS FECHA_PAGO,
        {{ parse_date_safe('TIME_STAMP') }} AS TIME_STAMP,
        2 AS RESPOND,

        -- ✅ Generate row number per CEDULA, ordering by FECHA_CORTE (most recent first)
        ROW_NUMBER() OVER (PARTITION BY {{ format_ID_EC('CEDULA') }} ORDER BY FECHA_CORTE DESC) AS rn

    FROM {{ source('raw_data', 'DATA_WAREHAUSE_BL_COBRANZA') }}
    QUALIFY rn = 1  -- ✅ Filter inside the CTE to keep only the most recent row per CEDULA
)

SELECT
    NOMBRE_COMPLETO,
    CEDULA,
    CORREO,
    DIRECCION,
    CELULAR,
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
    FECHA_PAGO,
    BUCKET,
    ESTADO_PRD,
    EDAD,
    VALIDACION,
    CAMPANIA,
    TIME_STAMP,
    NOMBRE,
    APELLIDO,
    RESPOND,
    CURRENT_DATE() AS FECHA_PROCESO
FROM source_data_cobranza
