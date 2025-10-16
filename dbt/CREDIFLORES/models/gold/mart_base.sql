{{ config(
    materialized = 'table',
    unique_key   = 'UNIQUE_ID',
    tags         = ['mart']
) }}

WITH src AS (
    SELECT * FROM {{ ref('cred_inc') }}
)

SELECT
    UNIQUE_ID,
    CEDULA,
    CELULAR,
    NOMBRE_COMPLETO,
    DIRECCION,

    -- 🔢 Montos (negativos → 0)
    GREATEST(VALOR_TOTAL_DEUDA,                         0) AS VALOR_TOTAL_DEUDA,
    GREATEST(SALDO_CAPITAL,                             0) AS SALDO_CAPITAL,
    GREATEST(PAGO_MINIMO,                               0) AS PAGO_MINIMO,
       CASE
      WHEN DIAS_MORA IS NULL THEN 0
      ELSE GREATEST(DIAS_MORA, 0)
    END                                AS DIAS_MORA,      -- (si prefieres mantener negativos, quítalo)
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
    GREATEST(CAP_VENCIDO_CV,                            0) AS CAP_VENCIDO_CV,
    GREATEST(INTERESES,                                 0) AS INTERESES,
    GREATEST(CALCULO_MORA,                              0) AS CALCULO_MORA,
    GREATEST(SEG_DE_VIDA_CV,                            0) AS SEG_DE_VIDA_CV,
    GREATEST(SALDO_CAPITAL_C,                           0) AS SALDO_CAPITAL_C,
    P_CONDONACION_CAPITAL,
    GREATEST(VR_MAX_COND_CAP,                           0) AS VR_MAX_COND_CAP,
    GREATEST(INTERESES_CTES,                            0) AS INTERESES_CTES,
    P_COND_INT,
    GREATEST(VR_MAX_COND_INT,                           0) AS VR_MAX_COND_INT,
    GREATEST(INTERESES_MORA,                            0) AS INTERESES_MORA,
    P_COND_INT_1,
    GREATEST(VR_MAX_COND_INT_M,                         0) AS VR_MAX_COND_INT_M,
    GREATEST(VR_MAX_COND,                               0) AS VR_MAX_COND,
    GREATEST(TOTAL_RECUP_CREDIFLORES,                   0) AS TOTAL_RECUP_CREDIFLORES,
    GREATEST(VR_HONORARIOS,                             0) AS VR_HONORARIOS,
    GREATEST(VR_HONORARIOS_PAGO_VALOR_TOTAL_DEUDA_,     0) AS VR_HONORARIOS_PAGO_VALOR_TOTAL_DEUDA_,
    GREATEST(VR_HONORARIOS_PAGO_VALOR_TOTAL_DEUDA_1,    0) AS VR_HONORARIOS_PAGO_VALOR_TOTAL_DEUDA_1,
    GREATEST(VL_MIN_PAGO,                               0) AS VL_MIN_PAGO,
    ANO_CASTIGO,
    F_CASTIGO,
    INSTANCIA_DE_COBRO,
    TIME_STAMP,
    NOMBRE,
    APELLIDO,
    RESPOND,
    POLITICA,
    SESION,
    SESSION_TIME,
    AUX_SOLUCION,
    FECHA_PROCESO,
    CAMPANA,

    -- 📊 Nuevos cálculos de cobro (sumandos ya recortados)
    GREATEST(VL_MIN_PAGO, 0)                                           AS VALOR_TOTAL_DEUDA_C,

    GREATEST(VALOR_TOTAL_DEUDA,                        0)
      + GREATEST(VR_HONORARIOS_PAGO_VALOR_TOTAL_DEUDA_, 0)             AS VALOR_TOTAL_DEUDA_V,

    GREATEST(PAGO_MINIMO,                              0)
      + GREATEST(VR_HONORARIOS_PAGO_VALOR_TOTAL_DEUDA_1, 0)            AS VALOR_MINIMO_DEUDA_V

FROM src
