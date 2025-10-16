{% snapshot union_snapshot %}
{{
  config(
    target_database="charlieserver-281513",
    target_schema="produbanco_dev",
    unique_key="CEDULA",
    strategy="timestamp",
    updated_at="TIME_STAMP"
  )
}}

WITH formatted AS (
  SELECT *,
         -- Prioriza RESPOND = 1, y si hay varias, ordena por TIME_STAMP DESC
         ROW_NUMBER() OVER (
            PARTITION BY CEDULA 
            ORDER BY 
              CASE WHEN RESPOND = 1 THEN 0 ELSE 1 END,
              TIME_STAMP DESC
         ) AS rn
  FROM {{ ref("union_tablas") }}
)

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
    CAST(TIME_STAMP AS TIMESTAMP) AS TIME_STAMP,
    NOMBRE,
    APELLIDO,
    RESPOND,
    VALIDACION,
    FECHA_PROCESO
FROM formatted
WHERE rn = 1

{% endsnapshot %}
