{% snapshot base_snapshot %}
{{
  config(
    target_database="charlieserver-281513",
    target_schema="crediflores_prod",
    unique_key="UNIQUE_ID",
    strategy="timestamp",
    updated_at="TIME_STAMP"
  )
}}

WITH formatted AS (
  SELECT
    A.* EXCEPT(rn),
    ROW_NUMBER() OVER (
      PARTITION BY UNIQUE_ID 
      ORDER BY 
        CASE WHEN RESPOND = 1 THEN 0 ELSE 1 END,
        TIME_STAMP DESC
    ) AS rn
  FROM {{ ref("base") }} A
)

SELECT 
    UNIQUE_ID,
    CEDULA,
    CELULAR,
    NOMBRE_COMPLETO,
    DIRECCION,
    VALOR_TOTAL_DEUDA,
    SALDO_CAPITAL,
    PAGO_MINIMO,
    DIAS_MORA,
    TARJETA,
    MARCA,
    CALIFICACION,
    TARJETA_DIG,
    TIPO_OPERACION,
    FECHA_CORTE,
    BUCKET,
    ESTADO_PRD,
    FECHA_PAGO,
    CAMPANIA,
    ESTADO_CONVENIO,
    CAP_VENCIDO_CV,
    INTERESES,
    CALCULO_MORA,
    SEG_DE_VIDA_CV,
    SALDO_CAPITAL_C,
    P_CONDONACION_CAPITAL,
    VR_MAX_COND_CAP,
    INTERESES_CTES,
    P_COND_INT,
    VR_MAX_COND_INT,
    INTERESES_MORA,
    P_COND_INT_1,
    VR_MAX_COND_INT_M,
    VR_MAX_COND,
    TOTAL_RECUP_CREDIFLORES,
    VR_HONORARIOS,
    VR_HONORARIOS_PAGO_VALOR_TOTAL_DEUDA_,
    VR_HONORARIOS_PAGO_VALOR_TOTAL_DEUDA_1,
    VL_MIN_PAGO,
    ANO_CASTIGO,
    F_CASTIGO,
    INSTANCIA_DE_COBRO,
    TIME_STAMP,
    NOMBRE,
    APELLIDO,
    EDAD,
    CAMPANA,
     CASE
      WHEN CAMPANA = 0 THEN 0
      ELSE RESPOND
    END                                           AS RESPOND,
    POLITICA,
    SESION,
    SESSION_TIME,
    AUX_SOLUCION,
    FECHA_PROCESO
FROM formatted
WHERE rn = 1

{% endsnapshot %}