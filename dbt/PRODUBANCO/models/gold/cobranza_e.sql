WITH cobranza_efectiva AS (
    SELECT DISTINCT CEDULA
    FROM {{ ref('diferencias') }}
    WHERE TIPO_CAMBIO = 'PAGO_INFERIDO'
),
intentos_unicos AS (
    SELECT DISTINCT CEDULA, MAX(CREATED_AT) AS LAST_INTENT
    FROM {{ ref('client_intents') }}
    WHERE INTENT IS NOT NULL -- Filtrar solo los registros que realmente tienen INTENT
    GROUP BY CEDULA
)
SELECT 
    ce.CEDULA,
    iu.LAST_INTENT AS CREATED_AT,
    CASE 
        WHEN iu.CEDULA IS NOT NULL THEN 'INTERACCION'
        ELSE 'SIN_INTERACCION'
    END AS INTERACCION
FROM cobranza_efectiva ce
LEFT JOIN intentos_unicos iu ON ce.CEDULA = iu.CEDULA