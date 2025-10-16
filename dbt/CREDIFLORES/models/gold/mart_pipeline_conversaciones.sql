-- =========================
-- 1) Resumen por día
-- =========================
WITH resumen_intents AS (
  SELECT
    TRIM(CAST(ci.id_cliente AS STRING))                            AS id_cliente,
    DATE(ci.created_at)                                            AS fecha_interaccion,
    MIN(ci.created_at)                                             AS primer_interaccion,
    MAX(ci.created_at)                                             AS ultima_interaccion,
    TIMESTAMP_DIFF(MAX(ci.created_at), MIN(ci.created_at), MINUTE) AS duracion_conversacion_min,

    -- Estado del producto (traído de la tabla base)
    ANY_VALUE(b.estado_prd)                                        AS estado_prd,

    -- Pipeline de intents del día (sin repetidos)
    STRING_AGG(DISTINCT i.intent ORDER BY i.intent)                AS pipeline_conversacion,

    -- Flags por intent
    MAX(CASE WHEN i.intent = 'Default Fallback Intent'     THEN 1 ELSE 0 END) AS flag_fallback,
    MAX(CASE WHEN i.intent = 'Default Welcome Intent'      THEN 1 ELSE 0 END) AS flag_welcome,
    MAX(CASE WHEN i.intent = 'agencia_cercana'             THEN 1 ELSE 0 END) AS flag_agencia_cercana,
    MAX(CASE WHEN i.intent = 'corresponsales_bancarios'    THEN 1 ELSE 0 END) AS flag_corresponsales_bancarios,
    MAX(CASE WHEN i.intent = 'datos_personales'            THEN 1 ELSE 0 END) AS flag_datos_personales,
    MAX(CASE WHEN i.intent = 'desempleo'                   THEN 1 ELSE 0 END) AS flag_desempleo,
    MAX(CASE WHEN i.intent = 'detalle_saldo'               THEN 1 ELSE 0 END) AS flag_detalle_saldo,
    MAX(CASE WHEN i.intent = 'detalle_saldo_val'           THEN 1 ELSE 0 END) AS flag_detalle_saldo_val,
    MAX(CASE WHEN i.intent = 'formas_pago'                 THEN 1 ELSE 0 END) AS flag_formas_pago,
    MAX(CASE WHEN i.intent = 'gracias'                     THEN 1 ELSE 0 END) AS flag_gracias,
    MAX(CASE WHEN i.intent = 'hablar_asesor'               THEN 1 ELSE 0 END) AS flag_hablar_asesor,
    MAX(CASE WHEN i.intent = 'mas_info'                    THEN 1 ELSE 0 END) AS flag_mas_info,
    MAX(CASE WHEN i.intent = 'menu'                        THEN 1 ELSE 0 END) AS flag_menu,
    MAX(CASE WHEN i.intent = 'num_incorrecto'              THEN 1 ELSE 0 END) AS flag_num_incorrecto,
    MAX(CASE WHEN i.intent = 'pago_pse'                    THEN 1 ELSE 0 END) AS flag_pago_pse,
    MAX(CASE WHEN i.intent = 'sp-cedula'                   THEN 1 ELSE 0 END) AS flag_sp_cedula,
    MAX(CASE WHEN i.intent = 'sp-pag-acepto-convenio'      THEN 1 ELSE 0 END) AS flag_sp_pag_acepto_convenio,
    MAX(CASE WHEN i.intent = 'sp-pag-firmar'               THEN 1 ELSE 0 END) AS flag_sp_pag_firmar,
    MAX(CASE WHEN i.intent = 'sp-prop'                     THEN 1 ELSE 0 END) AS flag_sp_prop,
    MAX(CASE WHEN i.intent = 'sp_pag'                      THEN 1 ELSE 0 END) AS flag_sp_pag,
    MAX(CASE WHEN i.intent = 'ya_pague'                    THEN 1 ELSE 0 END) AS flag_ya_pague
  FROM `charlieserver-281513.crediflores_prod.cred_clients_intents` ci
  LEFT JOIN `charlieserver-281513.crediflores_prod.cred_intents` i
    ON ci.id_intent = CAST(i.id AS STRING)
  LEFT JOIN `charlieserver-281513.crediflores_prod.base` b
    ON ci.id_cliente = CAST(b.UNIQUE_ID AS STRING)   -- 🔹 Ajusta si el join es por otra columna
  WHERE ci.id_cliente IS NOT NULL
  GROUP BY id_cliente, DATE(ci.created_at)
),

-- =========================
-- 2) Métrica de recurrencia
-- =========================
clientes_recurrentes AS (
  SELECT
    id_cliente,
    COUNT(DISTINCT fecha_interaccion) AS dias_interactuo,
    MIN(fecha_interaccion)            AS primer_contacto
  FROM resumen_intents
  GROUP BY id_cliente
),

-- =========================
-- 3) Join con dim_tiempo
-- =========================
final_con_join AS (
  SELECT
    ri.*,
    cr.dias_interactuo,
    CASE WHEN cr.dias_interactuo > 1 THEN 'Recurrente' ELSE 'Único' END AS tipo_cliente,
    CASE WHEN ri.fecha_interaccion = cr.primer_contacto THEN 1 ELSE 0 END AS es_primer_contacto,

    DATE(ri.primer_interaccion)                AS fecha_primer_intent,
    EXTRACT(HOUR FROM ri.primer_interaccion)   AS hora_dia,

    CASE 
      WHEN EXTRACT(HOUR FROM ri.primer_interaccion) BETWEEN 0 AND 5  THEN 'Madrugada'
      WHEN EXTRACT(HOUR FROM ri.primer_interaccion) BETWEEN 6 AND 11 THEN 'Mañana'
      WHEN EXTRACT(HOUR FROM ri.primer_interaccion) BETWEEN 12 AND 17 THEN 'Tarde'
      ELSE 'Noche'
    END AS bloque_horario,

    dt.anio,
    dt.trimestre,
    dt.mes,
    dt.nombre_mes,
    dt.semana_anio,
    dt.dia,
    dt.dia_semana,
    dt.nombre_dia,
    dt.dia_anio,
    dt.dia_semana_iso,
    dt.semana_iso,
    dt.anio_semana,
    dt.anio_mes,
    dt.es_fin_de_semana,
    dt.anio_mes_str,
    dt.anio_semana_str
  FROM resumen_intents ri
  LEFT JOIN clientes_recurrentes cr
    ON ri.id_cliente = cr.id_cliente
  LEFT JOIN `charlieserver-281513.crediflores_prod.dim_tiempo` dt
    ON ri.fecha_interaccion = dt.fecha_calendario
),

-- =========================
-- 4) Deduplicación final
-- =========================
dedup AS (
  SELECT
    final_con_join.*,
    ROW_NUMBER() OVER (
      PARTITION BY id_cliente, fecha_interaccion
      ORDER BY ultima_interaccion DESC
    ) AS rn
  FROM final_con_join
)

-- =========================
-- 5) Filtro por cliente y fechas
-- =========================
SELECT
  id_cliente,
  fecha_interaccion,
  primer_interaccion,
  ultima_interaccion,
  duracion_conversacion_min,
  pipeline_conversacion,

  estado_prd,  -- ✅ Ahora viene de la tabla base

  flag_fallback,
  flag_welcome,
  flag_agencia_cercana,
  flag_corresponsales_bancarios,
  flag_datos_personales,
  flag_desempleo,
  flag_detalle_saldo,
  flag_detalle_saldo_val,
  flag_formas_pago,
  flag_gracias,
  flag_hablar_asesor,
  flag_mas_info,
  flag_menu,
  flag_num_incorrecto,
  flag_pago_pse,
  flag_sp_cedula,
  flag_sp_pag_acepto_convenio,
  flag_sp_pag_firmar,
  flag_sp_prop,
  flag_sp_pag,
  flag_ya_pague,

  dias_interactuo,
  tipo_cliente,
  es_primer_contacto,
  fecha_primer_intent,
  hora_dia,
  bloque_horario,

  anio,
  trimestre,
  mes,
  nombre_mes,
  semana_anio,
  dia,
  dia_semana,
  nombre_dia,
  dia_anio,
  dia_semana_iso,
  semana_iso,
  anio_semana,
  anio_mes,
  es_fin_de_semana,
  anio_mes_str,
  anio_semana_str
FROM dedup
WHERE rn = 1
ORDER BY id_cliente, fecha_interaccion
