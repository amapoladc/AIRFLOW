/* =========================================================================
   REPORTE — Clientes que INTERACTUARON y PAGARON dentro de 15 días
   + Clientes con MENSAJE (template) SIN INTERACCIÓN y PAGO dentro de 15 días
   ========================================================================= */

/* ---------- 0 · Mensajes (template_tracking) por cliente/día ---------- */
WITH mensajes_enviados AS (
  SELECT
    CONCAT(CAST(t.cedula AS STRING), '-', CAST(t.tarjeta_dig AS STRING)) AS id_cliente,
    DATE(t.timestamp)                                                    AS fecha_mensaje,
    MIN(t.timestamp)                                                     AS primer_envio_ts,
    COUNT(*)                                                             AS mensajes_enviados_en_el_dia,
    COUNTIF(LOWER(t.status) IN ('sent','delivered','read'))              AS mensajes_validos_en_el_dia,
    -- Metadatos útiles del último mensaje del día
    ARRAY_AGG(STRUCT(t.templatename, t.category, t.status, t.message, t.timestamp)
              ORDER BY t.timestamp DESC LIMIT 1)[OFFSET(0)].templatename AS templatename,
    ARRAY_AGG(STRUCT(t.templatename, t.category, t.status, t.message, t.timestamp)
              ORDER BY t.timestamp DESC LIMIT 1)[OFFSET(0)].category     AS category,
    ARRAY_AGG(STRUCT(t.templatename, t.category, t.status, t.message, t.timestamp)
              ORDER BY t.timestamp DESC LIMIT 1)[OFFSET(0)].status       AS status,
    ARRAY_AGG(STRUCT(t.templatename, t.category, t.status, t.message, t.timestamp)
              ORDER BY t.timestamp DESC LIMIT 1)[OFFSET(0)].message      AS message
  FROM `charlieserver-281513.crediflores_prod.template_tracking` t
  WHERE t.cedula IS NOT NULL AND t.tarjeta_dig IS NOT NULL
  GROUP BY id_cliente, fecha_mensaje
),

/* ---------- 1 · Interacciones diarias ---------- */
interacciones_diarias AS (
  SELECT
    CAST(ci.id_cliente AS STRING)           AS id_cliente,
    DATE(ci.created_at)                     AS fecha_interaccion,
    MIN(ci.created_at)                      AS primera_interaccion,
    MAX(ci.created_at)                      AS ultima_interaccion,
    TIMESTAMP_DIFF(MAX(ci.created_at), MIN(ci.created_at), MINUTE) AS duracion_min,
    COUNT(*)                                AS mensajes_en_el_dia
  FROM `charlieserver-281513.crediflores_prod.cred_clients_intents` ci
  WHERE ci.id_cliente IS NOT NULL
  GROUP BY id_cliente, DATE(ci.created_at)
),

/* ---------- 2 · Pagos inferidos + validación por saldo_capital ---------- */
pagos_row AS (
  SELECT
    CAST(bs.UNIQUE_ID AS STRING)           AS id_cliente,
    bs.nombre_completo,
    UPPER(bs.estado_prd)                   AS estado_prd,  -- ESTADO PRD
    DATE(bs.time_stamp)                    AS fecha_pago,
    bs.time_stamp                          AS ts,
    EXTRACT(MONTH FROM bs.time_stamp)      AS mes_gestion, -- MES DE GESTIÓN

    SAFE_CAST(bs.dias_mora AS INT64)       AS dias_mora,
    LAG(SAFE_CAST(bs.dias_mora AS INT64)) OVER (
      PARTITION BY bs.UNIQUE_ID, bs.campana
      ORDER BY bs.time_stamp, bs.fecha_proceso
    ) AS dias_mora_prev,

    CASE 
      WHEN UPPER(bs.estado_prd) = 'CASTIGADO' 
        THEN SAFE_CAST(bs.vl_min_pago AS NUMERIC)
      ELSE SAFE_CAST(bs.pago_minimo AS NUMERIC) 
           + IFNULL(SAFE_CAST(bs.vr_honorarios_pago_valor_total_deuda_1 AS NUMERIC), 0)
    END AS valor_cobrar,

    LAG(
      CASE 
        WHEN UPPER(bs.estado_prd) = 'CASTIGADO' 
          THEN SAFE_CAST(bs.vl_min_pago AS NUMERIC)
        ELSE SAFE_CAST(bs.pago_minimo AS NUMERIC) 
             + IFNULL(SAFE_CAST(bs.vr_honorarios_pago_valor_total_deuda_1 AS NUMERIC), 0)
      END
    ) OVER (
      PARTITION BY bs.UNIQUE_ID, bs.campana
      ORDER BY bs.time_stamp, bs.fecha_proceso
    ) AS valor_prev,

    SAFE_CAST(bs.saldo_capital AS NUMERIC) AS saldo_capital,
    LAG(SAFE_CAST(bs.saldo_capital AS NUMERIC)) OVER (
      PARTITION BY bs.UNIQUE_ID, bs.campana
      ORDER BY bs.time_stamp, bs.fecha_proceso
    ) AS saldo_capital_prev
  FROM `charlieserver-281513.crediflores_prod.base_snapshot` bs
),

/* ---------- 3 · Cálculos por fila ---------- */
pagos_row_calc AS (
  SELECT p.*,
    CASE
      WHEN p.valor_cobrar < 0
        THEN COALESCE(p.valor_prev, 0) + ABS(p.valor_cobrar)
      ELSE GREATEST(COALESCE(p.valor_prev, 0) - p.valor_cobrar, 0)
    END AS monto_pago_row,

    (
      CASE
        WHEN p.valor_cobrar < 0
          THEN COALESCE(p.valor_prev, 0) + ABS(p.valor_cobrar)
        ELSE GREATEST(COALESCE(p.valor_prev, 0) - p.valor_cobrar, 0)
      END
    ) > 0 AS pago_inferido_por_monto_row,

    (COALESCE(p.saldo_capital_prev,0) > COALESCE(p.saldo_capital,0)) AS saldo_capital_baja_row,

    CAST(
      CASE 
        WHEN p.estado_prd = 'CASTIGADO' THEN
          CASE WHEN COALESCE(p.dias_mora_prev, p.dias_mora) <= 365 THEN 0.15 ELSE 0.20 END
        ELSE 
          CASE 
            WHEN COALESCE(p.dias_mora_prev, p.dias_mora) BETWEEN 1 AND 60 THEN 0.03
            WHEN COALESCE(p.dias_mora_prev, p.dias_mora) >= 61 THEN 0.05
            ELSE 0
          END
      END
    AS NUMERIC) AS comision_row,

    CAST(ROUND(
      (
        CASE
          WHEN p.valor_cobrar < 0
            THEN COALESCE(p.valor_prev, 0) + ABS(p.valor_cobrar)
          ELSE GREATEST(COALESCE(p.valor_prev, 0) - p.valor_cobrar, 0)
        END
      ) *
      CAST(
        CASE 
          WHEN p.estado_prd = 'CASTIGADO' THEN
            CASE WHEN COALESCE(p.dias_mora_prev, p.dias_mora) <= 365 THEN 0.15 ELSE 0.20 END
          ELSE 
            CASE 
              WHEN COALESCE(p.dias_mora_prev, p.dias_mora) BETWEEN 1 AND 60 THEN 0.03
              WHEN COALESCE(p.dias_mora_prev, p.dias_mora) >= 61 THEN 0.05
              ELSE 0
            END
        END AS NUMERIC
      ), 2
    ) AS NUMERIC) AS monto_comision_row
  FROM pagos_row p
),

/* ---------- 4 · Agregado por cliente/día ---------- */
pagos_diarios AS (
  SELECT
    id_cliente,
    ANY_VALUE(nombre_completo) AS nombre_completo,
    ANY_VALUE(estado_prd)      AS estado_prd,   -- ESTADO PRD
    ANY_VALUE(mes_gestion)     AS mes_gestion,  -- MES DE GESTIÓN
    fecha_pago,

    SUM(CASE WHEN pago_inferido_por_monto_row AND saldo_capital_baja_row THEN monto_pago_row ELSE 0 END)     AS total_pagado_dia,
    SUM(CASE WHEN pago_inferido_por_monto_row AND saldo_capital_baja_row THEN monto_comision_row ELSE 0 END) AS total_monto_comision_dia,

    ARRAY_AGG(
      IF(pago_inferido_por_monto_row AND saldo_capital_baja_row, STRUCT(ts, dias_mora), NULL)
      IGNORE NULLS ORDER BY ts DESC
    )[SAFE_OFFSET(0)].dias_mora AS ultimo_dias_mora_dia,

    ARRAY_AGG(
      IF(pago_inferido_por_monto_row AND saldo_capital_baja_row, STRUCT(ts, dias_mora_prev), NULL)
      IGNORE NULLS ORDER BY ts DESC
    )[SAFE_OFFSET(0)].dias_mora_prev AS penultimo_dias_mora_dia,

    ARRAY_AGG(
      IF(pago_inferido_por_monto_row AND saldo_capital_baja_row, STRUCT(ts, comision_row), NULL)
      IGNORE NULLS ORDER BY ts DESC LIMIT 1
    )[OFFSET(0)].comision_row AS ultimo_comision_dia,

    MAX(pago_inferido_por_monto_row AND saldo_capital_baja_row) AS hubo_pago_verdadero_dia
  FROM pagos_row_calc
  GROUP BY id_cliente, fecha_pago
),

/* ---------- 5 · Mantener solo días con pago verdadero ---------- */
pagos_verdaderos AS (
  SELECT
    id_cliente,
    nombre_completo,
    estado_prd,   -- ESTADO PRD
    mes_gestion,  -- MES DE GESTIÓN
    fecha_pago,
    total_pagado_dia,
    total_monto_comision_dia,
    ultimo_dias_mora_dia,
    penultimo_dias_mora_dia,
    ultimo_comision_dia
  FROM pagos_diarios
  WHERE hubo_pago_verdadero_dia = TRUE
),

/* ---------- 6A · Cobranza efectiva: INTERACCION_Y_PAGO ---------- */
final_interaccion_y_pago AS (
  SELECT
    i.fecha_interaccion                           AS fecha_evento,
    i.id_cliente,
    pv.nombre_completo,
    pv.estado_prd,          -- ESTADO PRD
    pv.mes_gestion,         -- MES DE GESTIÓN
    MIN(pv.fecha_pago) AS fecha_pago_efectivo,
    i.mensajes_en_el_dia,   -- # de interacciones ese día
    i.duracion_min,
    SUM(pv.total_pagado_dia)                                    AS total_pagado_15_dias,
    CAST(ROUND(SUM(pv.total_monto_comision_dia), 2) AS NUMERIC) AS monto_comision_15_dias,

    ARRAY_AGG(STRUCT(pv.fecha_pago, pv.penultimo_dias_mora_dia)
              ORDER BY pv.fecha_pago DESC LIMIT 1)[OFFSET(0)].penultimo_dias_mora_dia AS penultimo_dia_mora_15d,

    ARRAY_AGG(STRUCT(pv.fecha_pago, pv.ultimo_comision_dia)
              ORDER BY pv.fecha_pago DESC LIMIT 1)[OFFSET(0)].ultimo_comision_dia AS comision_correspondiente_ultima,

    'INTERACCION_Y_PAGO' AS cobranza_efectiva_tipo,

    -- Campos de template (no aplican aquí)
    CAST(NULL AS STRING) AS templatename,
    CAST(NULL AS STRING) AS category,
    CAST(NULL AS STRING) AS status,
    CAST(NULL AS STRING) AS message
  FROM interacciones_diarias i
  JOIN pagos_verdaderos pv
    ON i.id_cliente = pv.id_cliente
   AND pv.fecha_pago BETWEEN i.fecha_interaccion AND DATE_ADD(i.fecha_interaccion, INTERVAL 15 DAY)
  GROUP BY i.fecha_interaccion, i.id_cliente, pv.nombre_completo, pv.estado_prd, pv.mes_gestion, i.mensajes_en_el_dia, i.duracion_min
),

/* ---------- 6B · Cobranza efectiva: MENSAJE_SIN_INTERACCION_Y_PAGO ---------- */
final_mensaje_sin_interaccion_y_pago AS (
  SELECT
    m.fecha_mensaje                               AS fecha_evento,
    m.id_cliente,
    ANY_VALUE(pv.nombre_completo)                 AS nombre_completo,
    ANY_VALUE(pv.estado_prd)                      AS estado_prd,
    ANY_VALUE(pv.mes_gestion)                     AS mes_gestion,
    MIN(pv.fecha_pago)                            AS fecha_pago_efectivo,
    m.mensajes_validos_en_el_dia                  AS mensajes_en_el_dia,  -- # de mensajes válidos
    CAST(NULL AS INT64)                           AS duracion_min,
    SUM(pv.total_pagado_dia)                                    AS total_pagado_15_dias,
    CAST(ROUND(SUM(pv.total_monto_comision_dia), 2) AS NUMERIC) AS monto_comision_15_dias,

    ARRAY_AGG(STRUCT(pv.fecha_pago, pv.penultimo_dias_mora_dia)
              ORDER BY pv.fecha_pago DESC LIMIT 1)[OFFSET(0)].penultimo_dias_mora_dia AS penultimo_dia_mora_15d,

    ARRAY_AGG(STRUCT(pv.fecha_pago, pv.ultimo_comision_dia)
              ORDER BY pv.fecha_pago DESC LIMIT 1)[OFFSET(0)].ultimo_comision_dia AS comision_correspondiente_ultima,

    'MENSAJE_SIN_INTERACCION_Y_PAGO' AS cobranza_efectiva_tipo,

    -- Campos de template (sí aplican)
    ANY_VALUE(m.templatename) AS templatename,
    ANY_VALUE(m.category)     AS category,
    ANY_VALUE(m.status)       AS status,
    ANY_VALUE(m.message)      AS message
  FROM mensajes_enviados m
  JOIN pagos_verdaderos pv
    ON pv.id_cliente = m.id_cliente
   AND pv.fecha_pago BETWEEN m.fecha_mensaje AND DATE_ADD(m.fecha_mensaje, INTERVAL 15 DAY)
  LEFT JOIN interacciones_diarias i
    ON i.id_cliente = m.id_cliente
   AND i.fecha_interaccion BETWEEN m.fecha_mensaje AND DATE_ADD(m.fecha_mensaje, INTERVAL 15 DAY)
  WHERE m.mensajes_validos_en_el_dia > 0   -- asegura que el mensaje "llegó" (sent/delivered/read)
    AND i.id_cliente IS NULL               -- NO hubo interacción en la ventana
  GROUP BY m.fecha_mensaje, m.id_cliente, m.mensajes_validos_en_el_dia
),


/* ---------- 7 · Resultado combinado ---------- */
final_todos AS (
  SELECT * FROM final_interaccion_y_pago
  UNION ALL
  SELECT * FROM final_mensaje_sin_interaccion_y_pago  -- ← nombre correcto
),

/* ---------- 8 · ÚLTIMA FECHA por id_cliente ---------- */
ultimo_evento_por_cliente AS (
  SELECT
    f.*,
    ROW_NUMBER() OVER (
      PARTITION BY f.id_cliente
      ORDER BY
        f.fecha_evento DESC,          -- 1) última fecha del evento
        f.fecha_pago_efectivo DESC,   -- 2) desempate: último pago efectivo
        f.total_pagado_15_dias DESC   -- 3) desempate: mayor monto pagado
    ) AS rn
  FROM final_todos f
)

/* ---------- 9 · Salida final ---------- */
SELECT
  fecha_evento,
  id_cliente,
  nombre_completo,
  estado_prd,
  mes_gestion,
  fecha_pago_efectivo,
  mensajes_en_el_dia,
  duracion_min,
  total_pagado_15_dias,
  monto_comision_15_dias,
  penultimo_dia_mora_15d,
  comision_correspondiente_ultima,
  cobranza_efectiva_tipo,
  templatename,
  category,
  status,
  message
FROM ultimo_evento_por_cliente
WHERE rn = 1
ORDER BY id_cliente
