WITH Transiciones AS (
    SELECT
        ci.CEDULA,
        ci.INTENT AS intent_actual,
        LAG(ci.INTENT) OVER (
            PARTITION BY ci.CEDULA
            ORDER BY ci.CREATED_AT
        ) AS intent_anterior
    FROM {{ ref('client_intents') }} AS ci
)
SELECT
    intent_anterior,
    intent_actual,
    COUNT(*) AS cantidad
FROM Transiciones
WHERE intent_anterior IS NOT NULL
GROUP BY
    intent_anterior,
    intent_actual
ORDER BY cantidad DESC


