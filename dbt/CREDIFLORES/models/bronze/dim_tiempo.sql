{{ config(materialized='table') }}

WITH fechas AS (
  SELECT
      DATE(day) AS fecha_calendario
  FROM UNNEST(
    GENERATE_DATE_ARRAY('2023-01-01', '2030-12-31')
  ) AS day
),
horas AS (
  SELECT hour FROM UNNEST(GENERATE_ARRAY(0, 23)) AS hour
),
fechas_horas AS (
  SELECT
    fecha_calendario,
    hour,
    TIMESTAMP(DATETIME(fecha_calendario, TIME(hour, 0, 0))) AS fecha_hora
  FROM fechas
  CROSS JOIN horas
)

SELECT
  CAST(FORMAT_DATE('%Y%m%d', fecha_calendario) AS INT64) AS d_tiempo,
  fecha_calendario,
  hour AS hora_dia,
  -- Clasificación por bloque horario
  CASE
    WHEN hour BETWEEN 0 AND 5 THEN 'Madrugada'
    WHEN hour BETWEEN 6 AND 11 THEN 'Mañana'
    WHEN hour BETWEEN 12 AND 17 THEN 'Tarde'
    ELSE 'Noche'
  END AS bloque_horario,

  EXTRACT(YEAR FROM fecha_calendario) AS anio,
  EXTRACT(QUARTER FROM fecha_calendario) AS trimestre,
  EXTRACT(MONTH FROM fecha_calendario) AS mes,
  CASE EXTRACT(MONTH FROM fecha_calendario)
    WHEN 1 THEN 'Enero' WHEN 2 THEN 'Febrero' WHEN 3 THEN 'Marzo'
    WHEN 4 THEN 'Abril' WHEN 5 THEN 'Mayo' WHEN 6 THEN 'Junio'
    WHEN 7 THEN 'Julio' WHEN 8 THEN 'Agosto' WHEN 9 THEN 'Septiembre'
    WHEN 10 THEN 'Octubre' WHEN 11 THEN 'Noviembre' WHEN 12 THEN 'Diciembre'
  END AS nombre_mes,
  EXTRACT(WEEK FROM fecha_calendario) AS semana_anio,
  EXTRACT(DAY FROM fecha_calendario) AS dia,
  EXTRACT(DAYOFWEEK FROM fecha_calendario) AS dia_semana,
  CASE EXTRACT(DAYOFWEEK FROM fecha_calendario)
    WHEN 1 THEN 'Domingo' WHEN 2 THEN 'Lunes' WHEN 3 THEN 'Martes'
    WHEN 4 THEN 'Miércoles' WHEN 5 THEN 'Jueves' WHEN 6 THEN 'Viernes' WHEN 7 THEN 'Sábado'
  END AS nombre_dia,
  EXTRACT(DAYOFYEAR FROM fecha_calendario) AS dia_anio,
  CAST(FORMAT_DATE('%w', fecha_calendario) AS INT64) AS dia_semana_iso,
  EXTRACT(ISOWEEK FROM fecha_calendario) AS semana_iso,
  EXTRACT(YEAR FROM fecha_calendario) * 100 + EXTRACT(WEEK FROM fecha_calendario) AS anio_semana,
  EXTRACT(YEAR FROM fecha_calendario) * 100 + EXTRACT(MONTH FROM fecha_calendario) AS anio_mes,
  CASE WHEN EXTRACT(DAYOFWEEK FROM fecha_calendario) IN (1, 7) THEN TRUE ELSE FALSE END AS es_fin_de_semana,
  CASE 
    WHEN EXTRACT(MONTH FROM fecha_calendario) = 12 AND EXTRACT(DAY FROM fecha_calendario) = 25 THEN TRUE
    WHEN EXTRACT(MONTH FROM fecha_calendario) = 1 AND EXTRACT(DAY FROM fecha_calendario) = 1 THEN TRUE
    ELSE FALSE
  END AS es_festivo,
  FORMAT_DATE('%Y-%m', fecha_calendario) AS anio_mes_str,
  FORMAT_DATE('%Y-W%W', fecha_calendario) AS anio_semana_str

FROM fechas_horas
