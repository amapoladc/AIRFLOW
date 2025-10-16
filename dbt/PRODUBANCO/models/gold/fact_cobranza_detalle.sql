{{ config(
    materialized='incremental',
    unique_key=['CEDULA', 'TIME_STAMP']
) }}

WITH HonorarioLimits AS (
    SELECT 1 AS Dias_de_Mora_Min, 15 AS Dias_de_Mora_Max, 0.37 AS Honorario_Min, 0.37 AS Honorario_Max
    UNION ALL SELECT 16, 30, 3.11, 7.92
    UNION ALL SELECT 31, 60, 5.18, 17.83
    UNION ALL SELECT 61, 90, 6.91, 25.77
    UNION ALL SELECT 91, 120, 8.64, 29.03
    UNION ALL SELECT 121, 180, 8.64, 29.03
    UNION ALL SELECT 181, 270, 8.64, 29.03
    UNION ALL SELECT 271, 9999, 8.64, 29.03  
),

/* --------------------------------------------------------------------------------------
   1) Take the base "snapshot" data, including first/last timestamps per CEDULA + CAMPANIA
   -------------------------------------------------------------------------------------- */
base_data AS (
    SELECT
        GENERATE_UUID() AS ID_DEUDA,
        CEDULA,
        -- Keep d_tiempo/FECHA_PROCESO if desired:
        CAST(FORMAT_DATE('%Y%m%d', TIME_STAMP) AS INT64) AS d_tiempo,
        DATE(TIME_STAMP) AS FECHA_PROCESO,
        TIME_STAMP,
        PAGO_MINIMO,
        DIAS_MORA,
        CALIFICACION,
        RESPOND,
        BUCKET,
        ESTADO_CONVENIO,
        CAMPANIA,
        MIN(TIME_STAMP) OVER (PARTITION BY CEDULA, CAMPANIA) AS PRIMERA_FECHA,
        MAX(TIME_STAMP) OVER (PARTITION BY CEDULA, CAMPANIA) AS ULTIMA_FECHA
    FROM {{ ref('union_snapshot') }}
),

/* -----------------------------------------------------------------------------------
   2) Calculate difference logic, adopting the approach from your second query:
      - DIFERENCIA = (previous PAGO_MINIMO) - (current PAGO_MINIMO)
      - Positive difference => Payment
      - Negative difference => Increase
   ----------------------------------------------------------------------------------- */
diferencias AS (
    SELECT
        bd.ID_DEUDA,
        bd.CEDULA,
        bd.d_tiempo,
        bd.FECHA_PROCESO,
        bd.TIME_STAMP,
        bd.PAGO_MINIMO,
        bd.DIAS_MORA,
        bd.CALIFICACION,
        bd.RESPOND,
        bd.CAMPANIA,
        bd.BUCKET,
        bd.ESTADO_CONVENIO,

        -- Previous PAGO_MINIMO within the same (CEDULA, CAMPANIA)
        LAG(bd.PAGO_MINIMO) OVER (
            PARTITION BY bd.CEDULA, bd.CAMPANIA
            ORDER BY bd.TIME_STAMP
        ) AS PAGO_MINIMO_PREVIO,

        -- Main difference calculation: previous minus current
        COALESCE(
            LAG(bd.PAGO_MINIMO) OVER (
                PARTITION BY bd.CEDULA, bd.CAMPANIA
                ORDER BY bd.TIME_STAMP
            ), 0
        ) - bd.PAGO_MINIMO AS DIFERENCIA,

        -- Payment amount if DIFERENCIA > 0
        CASE 
            WHEN ROW_NUMBER() OVER (
                    PARTITION BY bd.CEDULA, bd.CAMPANIA
                    ORDER BY bd.TIME_STAMP
                 ) > 1
                 AND COALESCE(
                     LAG(bd.PAGO_MINIMO) OVER (
                         PARTITION BY bd.CEDULA, bd.CAMPANIA
                         ORDER BY bd.TIME_STAMP
                     ), 0
                   ) - bd.PAGO_MINIMO > 0
            THEN COALESCE(
                     LAG(bd.PAGO_MINIMO) OVER (
                         PARTITION BY bd.CEDULA, bd.CAMPANIA
                         ORDER BY bd.TIME_STAMP
                     ), 0
                  ) - bd.PAGO_MINIMO
            ELSE 0
        END AS MONTO_PAGO,

        -- Increase amount if DIFERENCIA < 0
        CASE 
            WHEN ROW_NUMBER() OVER (
                    PARTITION BY bd.CEDULA, bd.CAMPANIA
                    ORDER BY bd.TIME_STAMP
                 ) > 1
                 AND COALESCE(
                     LAG(bd.PAGO_MINIMO) OVER (
                         PARTITION BY bd.CEDULA, bd.CAMPANIA
                         ORDER BY bd.TIME_STAMP
                     ), 0
                   ) - bd.PAGO_MINIMO < 0
            THEN bd.PAGO_MINIMO
                 - COALESCE(
                     LAG(bd.PAGO_MINIMO) OVER (
                         PARTITION BY bd.CEDULA, bd.CAMPANIA
                         ORDER BY bd.TIME_STAMP
                     ), 0
                   )
            ELSE 0
        END AS AUMENTO_DEUDA,

        -- Type of change
        CASE
            WHEN ROW_NUMBER() OVER (
                     PARTITION BY bd.CEDULA, bd.CAMPANIA
                     ORDER BY bd.TIME_STAMP
                 ) > 1
                 AND COALESCE(
                     LAG(bd.PAGO_MINIMO) OVER (
                         PARTITION BY bd.CEDULA, bd.CAMPANIA
                         ORDER BY bd.TIME_STAMP
                     ), 0
                   ) - bd.PAGO_MINIMO > 0
            THEN 'PAGO_INFERIDO'
            WHEN ROW_NUMBER() OVER (
                     PARTITION BY bd.CEDULA, bd.CAMPANIA
                     ORDER BY bd.TIME_STAMP
                 ) > 1
                 AND COALESCE(
                     LAG(bd.PAGO_MINIMO) OVER (
                         PARTITION BY bd.CEDULA, bd.CAMPANIA
                         ORDER BY bd.TIME_STAMP
                     ), 0
                   ) - bd.PAGO_MINIMO < 0
            THEN 'AUMENTO_DEUDA'
            ELSE 'SIN_CAMBIO'
        END AS TIPO_CAMBIO,

        -- Type of management
        CASE 
            WHEN bd.RESPOND = 1 THEN 'CONVENIO'
            WHEN bd.RESPOND = 2 THEN 'COBRANZA'
            ELSE 'OTRO'
        END AS TIPO_GESTION
    FROM base_data bd
),

/* -----------------------------------------------------------
   3) Cálculo de Fee_Percentage (unchanged)
   ----------------------------------------------------------- */
calculo_fees AS (
    SELECT
        d.*,
        CASE
            WHEN d.TIPO_CAMBIO = 'PAGO_INFERIDO' THEN
                CASE
                    WHEN d.DIAS_MORA BETWEEN 1 AND 15 THEN 0.00
                    WHEN d.DIAS_MORA BETWEEN 16 AND 30 THEN 
                        CASE 
                            WHEN d.MONTO_PAGO BETWEEN 20 AND 599.99 THEN 1.61
                            WHEN d.MONTO_PAGO BETWEEN 600 AND 1499.99 THEN 1.61
                            WHEN d.MONTO_PAGO > 1500 THEN 1.71
                            ELSE 0
                        END
                    WHEN d.DIAS_MORA BETWEEN 31 AND 60 THEN 
                        CASE 
                            WHEN d.MONTO_PAGO BETWEEN 20 AND 599.99 THEN 2.59
                            WHEN d.MONTO_PAGO BETWEEN 600 AND 1499.99 THEN 2.59
                            WHEN d.MONTO_PAGO > 1500 THEN 1.73
                            ELSE 0
                        END
                    WHEN d.DIAS_MORA BETWEEN 61 AND 90 THEN 
                        CASE 
                            WHEN d.MONTO_PAGO BETWEEN 20 AND 599.99 THEN 4.32
                            WHEN d.MONTO_PAGO BETWEEN 600 AND 1499.99 THEN 4.32
                            WHEN d.MONTO_PAGO > 1500 THEN 2.59
                            ELSE 0
                        END
                    WHEN d.DIAS_MORA BETWEEN 91 AND 120 THEN 
                        CASE 
                            WHEN d.MONTO_PAGO BETWEEN 20 AND 599.99 THEN 6.91
                            WHEN d.MONTO_PAGO BETWEEN 600 AND 1499.99 THEN 6.91
                            WHEN d.MONTO_PAGO > 1500 THEN 5.18
                            ELSE 0
                        END
                    WHEN d.DIAS_MORA BETWEEN 121 AND 180 THEN
                        CASE
                            WHEN d.MONTO_PAGO BETWEEN 20 AND 599.99 THEN 6.91
                            WHEN d.MONTO_PAGO BETWEEN 600 AND 1499.99 THEN 6.91
                            WHEN d.MONTO_PAGO > 1500 THEN 5.18
                            ELSE 0
                        END
                    WHEN d.DIAS_MORA BETWEEN 181 AND 270 THEN 
                        CASE 
                            WHEN d.MONTO_PAGO BETWEEN 20 AND 599.99 THEN 9.07
                            WHEN d.MONTO_PAGO BETWEEN 600 AND 1499.99 THEN 9.07
                            WHEN d.MONTO_PAGO > 1500 THEN 8.21
                            ELSE 0
                        END
                    WHEN d.DIAS_MORA > 270 THEN 
                        CASE 
                            WHEN d.MONTO_PAGO BETWEEN 20 AND 599.99 THEN 9.07
                            WHEN d.MONTO_PAGO BETWEEN 600 AND 1499.99 THEN 9.07
                            WHEN d.MONTO_PAGO > 1500 THEN 8.21
                            ELSE 0
                        END
                    ELSE 0
                END
            ELSE 0
        END AS Fee_Percentage
    FROM diferencias d
),

/* -----------------------------------------------------------
   4) Cálculo de Percentage_Charge (unchanged)
   ----------------------------------------------------------- */
calculo_charges AS (
    SELECT
        cf.*,
        CASE 
            WHEN cf.TIPO_CAMBIO = 'PAGO_INFERIDO'
                 AND (cf.MONTO_PAGO * cf.Fee_Percentage)/100 = 0 THEN 0.37
            ELSE (cf.MONTO_PAGO * cf.Fee_Percentage)/100
        END AS Percentage_Charge
    FROM calculo_fees cf
),

/* -----------------------------------------------------------
   5) Aplicación de límites y cálculo final del honorario (unchanged)
   ----------------------------------------------------------- */
final AS (
    SELECT
        wc.*,
        CASE
            WHEN wc.TIPO_CAMBIO <> 'PAGO_INFERIDO' THEN 0
            WHEN wc.Percentage_Charge < hl.Honorario_Min THEN hl.Honorario_Min
            WHEN wc.Percentage_Charge > hl.Honorario_Max THEN hl.Honorario_Max
            ELSE wc.Percentage_Charge
        END AS Honorario
    FROM calculo_charges wc
    LEFT JOIN HonorarioLimits hl
        ON wc.DIAS_MORA BETWEEN hl.Dias_de_Mora_Min AND hl.Dias_de_Mora_Max
)

SELECT *
FROM final
