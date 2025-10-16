WITH src AS (
    SELECT * FROM {{ ref('cred_inc') }}
)

SELECT
    UNIQUE_ID,
    CEDULA,
    CELULAR,

    -- Primer nombre: primera palabra de NOMBRE
    SPLIT(NOMBRE, ' ')[OFFSET(0)]                        AS PRIMER_NOMBRE,

    -- Primer apellido si tiene más de 3 letras
    CASE
        WHEN LENGTH(SPLIT(APELLIDO, ' ')[OFFSET(0)]) > 3
             THEN SPLIT(APELLIDO, ' ')[OFFSET(0)]
        ELSE NULL
    END                                                  AS PRIMER_APELLIDO,

    -- Nombre completo reconstruido
    NOMBRE_COMPLETO,

    -- 🔢 Valores monetarios: negativos → 0
    {{ format_cop('GREATEST(VALOR_TOTAL_DEUDA, 0)') }}                                     AS VALOR_TOTAL_DEUDA,
    {{ format_cop('GREATEST(VR_HONORARIOS_PAGO_VALOR_TOTAL_DEUDA_, 0)') }}                 AS VR_HONORARIOS_PAGO_VALOR_TOTAL_DEUDA_,
    {{ format_cop('GREATEST(PAGO_MINIMO, 0)') }}                                           AS PAGO_MINIMO,
    {{ format_cop('GREATEST(VL_MIN_PAGO, 0)') }}                                           AS VL_MIN_PAGO,

    -- Cálculos adicionales con clipping a 0 antes de sumar
    {{ format_cop('GREATEST(VL_MIN_PAGO, 0)') }}                                           AS VALOR_TOTAL_DEUDA_C,
    {{ format_cop('GREATEST(VALOR_TOTAL_DEUDA, 0) + GREATEST(VR_HONORARIOS_PAGO_VALOR_TOTAL_DEUDA_, 0)') }}
                                                                                           AS VALOR_TOTAL_DEUDA_V,
    {{ format_cop('GREATEST(PAGO_MINIMO, 0) + GREATEST(VR_HONORARIOS_PAGO_VALOR_TOTAL_DEUDA_1, 0)') }}
                                                                                           AS VALOR_MINIMO_DEUDA_V,

    -- Información adicional
    DIAS_MORA,
    CALIFICACION,
    TARJETA_DIG,
    TIPO_OPERACION,
    RESPOND,
    POLITICA,
    AUX_SOLUCION,
    CAMPANA

FROM src
WHERE
      REGEXP_CONTAINS(CAST(CELULAR AS STRING), r'^573\d{9}$')   -- Celular colombiano
   OR REGEXP_CONTAINS(CAST(CELULAR AS STRING), r'^5939\d{8}$')  -- Celular ecuatoriano

