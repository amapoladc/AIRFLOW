/* ---------- 1 · Casts y campos crudos ---------- */
WITH source_casted AS (
  SELECT
    GENERATE_UUID() AS id_deuda,        -- Identificador único de la fila
    cedula,
    celular,
    nombre_completo,
    direccion,

    /* Conversiones a tipo NUMERIC para evitar errores en cálculos */
    SAFE_CAST(valor_total_deuda AS NUMERIC)                      AS valor_total_deuda,
    SAFE_CAST(saldo_capital     AS NUMERIC)                      AS saldo_capital,
    SAFE_CAST(pago_minimo       AS NUMERIC)                      AS pago_minimo_raw,
    SAFE_CAST(vl_min_pago       AS NUMERIC)                      AS vl_min_pago_raw,
    SAFE_CAST(vr_honorarios_pago_valor_total_deuda_1 AS NUMERIC) AS hono_pago_raw,

    /* Otros campos que no requieren transformación */
    dias_mora,
    tarjeta,
    marca,
    calificacion,
    tarjeta_dig,
    tipo_operacion,
    fecha_corte,
    bucket,
    estado_prd,
    edad,
    fecha_pago,
    campana,
    estado_convenio,
    ano_castigo,
    f_castigo,
    instancia_de_cobro,
    time_stamp,
    fecha_proceso,

    /* Primer y último timestamp por cliente/campaña */
    MIN(time_stamp) OVER (PARTITION BY cedula, campana) AS primera_fecha,
    MAX(time_stamp) OVER (PARTITION BY cedula, campana) AS ultima_fecha
  FROM `charlieserver-281513.crediflores_prod.base_snapshot`
),

/* ---------- 2 · Cálculo de valor a cobrar ---------- */
base_data AS (
  SELECT
    sc.*,
    CASE
      WHEN UPPER(sc.estado_prd) = 'CASTIGADO' 
        THEN sc.vl_min_pago_raw                                 -- Si está castigado, solo el vl_min_pago
      ELSE sc.pago_minimo_raw + IFNULL(sc.hono_pago_raw,0)      -- Si no, pago mínimo + honorarios
    END AS valor_cobrar
  FROM source_casted sc
),

/* ---------- 3 · Diferencias vs. registro previo ---------- */
diferencias AS (
  SELECT
    bd.*,

    /* Valor del registro previo (mismo cliente/campaña) */
    LAG(bd.valor_cobrar) OVER (
      PARTITION BY bd.cedula, bd.campana
      ORDER BY bd.time_stamp, bd.fecha_proceso
    ) AS valor_prev,

    /* Días de mora del registro previo */
    LAG(bd.dias_mora) OVER (
      PARTITION BY bd.cedula, bd.campana
      ORDER BY bd.time_stamp, bd.fecha_proceso
    ) AS dias_mora_prev,

    /* Identifica el último registro por cliente/campaña */
    ROW_NUMBER() OVER (
      PARTITION BY bd.cedula, bd.campana
      ORDER BY bd.time_stamp DESC, bd.fecha_proceso DESC
    ) AS rn_desc
  FROM base_data bd
)

/* ---------- 4 · Resultados finales ---------- */
SELECT
  d.*,

  /* Flag para identificar pagos que superan el saldo */
  d.valor_cobrar < 0 AS es_pago_excesivo,

  /* Diferencia entre valor previo y actual (ajustada para pagos excesivos) */
  CASE 
    WHEN d.valor_cobrar < 0 
      THEN COALESCE(d.valor_prev,0) + ABS(d.valor_cobrar)
    ELSE COALESCE(d.valor_prev,0) - d.valor_cobrar
  END AS diferencia,

  /* Monto de pago inferido (incluye excedente si aplica) */
  CASE 
    WHEN d.valor_cobrar < 0 
      THEN COALESCE(d.valor_prev,0) + ABS(d.valor_cobrar)
    ELSE GREATEST(COALESCE(d.valor_prev,0) - d.valor_cobrar,0)
  END AS monto_pago,

  /* Aumento de deuda (cuando el valor actual es mayor al previo) */
  GREATEST(d.valor_cobrar - COALESCE(d.valor_prev,0),0) AS aumento_deuda,

  /* Monto del excedente (cuando hay sobrepago) */
  ABS(CASE WHEN d.valor_cobrar < 0 THEN d.valor_cobrar END) AS monto_excedente,

  /* Clasificación del tipo de cambio */
  CASE
    WHEN d.valor_cobrar < 0  AND (COALESCE(d.valor_prev,0) - d.valor_cobrar) > 0 THEN 'PAGO_EXCESIVO'
    WHEN d.rn_desc = 1 THEN 'ULTIMO_REGISTRO'
    WHEN d.valor_prev IS NULL THEN 'PRIMER_REGISTRO'
    WHEN COALESCE(d.valor_prev,0) - d.valor_cobrar > 0 THEN 'PAGO_INFERIDO'
    WHEN COALESCE(d.valor_prev,0) - d.valor_cobrar < 0 THEN 'AUMENTO_DEUDA'
    ELSE 'SIN_CAMBIO'
  END AS tipo_cambio,

  /* Porcentaje de comisión basado en estado y mora */
  CASE 
    WHEN UPPER(d.estado_prd) = 'CASTIGADO' THEN
      CASE 
        WHEN COALESCE(d.dias_mora_prev, d.dias_mora) <= 365 THEN 0.15 
        ELSE 0.20 
      END
    ELSE 
      CASE 
        WHEN COALESCE(d.dias_mora_prev, d.dias_mora) BETWEEN 1 AND 60 THEN 0.03
        WHEN COALESCE(d.dias_mora_prev, d.dias_mora) >= 61 THEN 0.05
        ELSE 0
      END
  END AS comision,

  /* Monto de la comisión calculada sobre el pago */
  (
    CASE 
      WHEN d.valor_cobrar < 0 
        THEN COALESCE(d.valor_prev,0) + ABS(d.valor_cobrar)
      ELSE GREATEST(COALESCE(d.valor_prev,0) - d.valor_cobrar,0)
    END
  ) *
  CASE 
    WHEN UPPER(d.estado_prd) = 'CASTIGADO' THEN
      CASE 
        WHEN COALESCE(d.dias_mora_prev, d.dias_mora) <= 365 THEN 0.15 
        ELSE 0.20 
      END
    ELSE 
      CASE 
        WHEN COALESCE(d.dias_mora_prev, d.dias_mora) BETWEEN 1 AND 60 THEN 0.03
        WHEN COALESCE(d.dias_mora_prev, d.dias_mora) >= 61 THEN 0.05
        ELSE 0
      END
  END AS monto_comision

FROM diferencias d
