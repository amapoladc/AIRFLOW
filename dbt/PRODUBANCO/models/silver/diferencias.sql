WITH base_data AS (
    SELECT
        ID_DEUDA,
        CEDULA,
        TIME_STAMP,
        PAGO_MINIMO,
        DIAS_MORA,
        CALIFICACION,
        RESPOND,
        BUCKET,
        ESTADO_CONVENIO,
        CAMPANIA,
        -- Marcas de tiempo inicial y final por cédula y campaña
        MIN(TIME_STAMP) OVER (
            PARTITION BY CEDULA, CAMPANIA
        ) AS PRIMERA_FECHA,
        MAX(TIME_STAMP) OVER (
            PARTITION BY CEDULA, CAMPANIA
        ) AS ULTIMA_FECHA
    FROM {{ ref('deudas') }}
),

-- Identificamos el primer y último registro por cédula y campaña
rn_cte AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY CEDULA, CAMPANIA
            ORDER BY TIME_STAMP
        ) AS rn_asc,   -- primera ocurrencia en esa campaña
        ROW_NUMBER() OVER (
            PARTITION BY CEDULA, CAMPANIA
            ORDER BY TIME_STAMP DESC
        ) AS rn_desc   -- última ocurrencia en esa campaña
    FROM base_data
),

diferencias AS (
    SELECT
        bd.ID_DEUDA,
        bd.CEDULA,
        bd.TIME_STAMP,
        bd.PAGO_MINIMO,
        bd.DIAS_MORA,
        bd.CALIFICACION,
        bd.RESPOND,
        bd.CAMPANIA,
        bd.BUCKET,
        bd.ESTADO_CONVENIO,

        -- Pago mínimo previo dentro de la misma cédula y campaña
        LAG(bd.PAGO_MINIMO) OVER (
            PARTITION BY bd.CEDULA, bd.CAMPANIA
            ORDER BY bd.TIME_STAMP
        ) AS PAGO_MINIMO_PREVIO,

        -- Diferencia (positiva si hay pago, negativa si aumenta la deuda)
        COALESCE(
            LAG(bd.PAGO_MINIMO) OVER (
                PARTITION BY bd.CEDULA, bd.CAMPANIA
                ORDER BY bd.TIME_STAMP
            ), 0) - bd.PAGO_MINIMO AS DIFERENCIA,

        -- Monto de pago (solo se calcula si la diferencia es > 0)
        CASE 
            WHEN ROW_NUMBER() OVER (
                     PARTITION BY bd.CEDULA, bd.CAMPANIA
                     ORDER BY bd.TIME_STAMP
                 ) > 1
                 AND COALESCE(
                     LAG(bd.PAGO_MINIMO) OVER (
                         PARTITION BY bd.CEDULA, bd.CAMPANIA
                         ORDER BY bd.TIME_STAMP
                     ), 0) - bd.PAGO_MINIMO > 0 
            THEN COALESCE(
                     LAG(bd.PAGO_MINIMO) OVER (
                         PARTITION BY bd.CEDULA, bd.CAMPANIA
                         ORDER BY bd.TIME_STAMP
                     ), 0) - bd.PAGO_MINIMO
            ELSE 0
        END AS MONTO_PAGO,

        -- Monto de aumento de deuda (captura el valor cuando la deuda aumenta)
        CASE 
            WHEN ROW_NUMBER() OVER (
                     PARTITION BY bd.CEDULA, bd.CAMPANIA
                     ORDER BY bd.TIME_STAMP
                 ) > 1
                 AND COALESCE(
                     LAG(bd.PAGO_MINIMO) OVER (
                         PARTITION BY bd.CEDULA, bd.CAMPANIA
                         ORDER BY bd.TIME_STAMP
                     ), 0) - bd.PAGO_MINIMO < 0 
            THEN bd.PAGO_MINIMO - COALESCE(
                     LAG(bd.PAGO_MINIMO) OVER (
                         PARTITION BY bd.CEDULA, bd.CAMPANIA
                         ORDER BY bd.TIME_STAMP
                     ), 0)
            ELSE 0
        END AS AUMENTO_DEUDA,

        -- Tipo de cambio (pago inferido, aumento de deuda o sin cambio)
        CASE
            WHEN ROW_NUMBER() OVER (
                     PARTITION BY bd.CEDULA, bd.CAMPANIA
                     ORDER BY bd.TIME_STAMP
                 ) > 1
                 AND COALESCE(
                     LAG(bd.PAGO_MINIMO) OVER (
                         PARTITION BY bd.CEDULA, bd.CAMPANIA
                         ORDER BY bd.TIME_STAMP
                     ), 0) - bd.PAGO_MINIMO > 0
            THEN 'PAGO_INFERIDO'
            WHEN ROW_NUMBER() OVER (
                     PARTITION BY bd.CEDULA, bd.CAMPANIA
                     ORDER BY bd.TIME_STAMP
                 ) > 1
                 AND COALESCE(
                     LAG(bd.PAGO_MINIMO) OVER (
                         PARTITION BY bd.CEDULA, bd.CAMPANIA
                         ORDER BY bd.TIME_STAMP
                     ), 0) - bd.PAGO_MINIMO < 0
            THEN 'AUMENTO_DEUDA'
            ELSE 'SIN_CAMBIO'
        END AS TIPO_CAMBIO
    FROM base_data bd
),
final AS (
    SELECT
        d.ID_DEUDA,
        d.CEDULA,
        d.TIME_STAMP,
        d.PAGO_MINIMO,

        -- En lugar de usar rn_asc y rn_desc, usamos MIN/MAX con OVER:
        CASE 
            WHEN d.TIME_STAMP = MIN(d.TIME_STAMP) OVER (
                PARTITION BY d.CEDULA, d.CAMPANIA
            ) 
            THEN d.PAGO_MINIMO
            ELSE NULL
        END AS PAGO_MINIMO_INICIAL,

        CASE 
            WHEN d.TIME_STAMP = MAX(d.TIME_STAMP) OVER (
                PARTITION BY d.CEDULA, d.CAMPANIA
            )
            THEN d.PAGO_MINIMO
            ELSE NULL
        END AS PAGO_MINIMO_ULTIMO,

        d.PAGO_MINIMO_PREVIO,
        d.DIFERENCIA,
        d.MONTO_PAGO,
        d.AUMENTO_DEUDA,
        d.TIPO_CAMBIO,
        d.DIAS_MORA,
        d.CALIFICACION,
        d.RESPOND,
        d.CAMPANIA,
        d.BUCKET,
        d.ESTADO_CONVENIO,
        CASE 
            WHEN d.RESPOND = 1 THEN 'CONVENIO'
            WHEN d.RESPOND = 2 THEN 'COBRANZA'
            ELSE 'OTRO'
        END AS TIPO_GESTION
    FROM diferencias d
)

SELECT *
FROM final