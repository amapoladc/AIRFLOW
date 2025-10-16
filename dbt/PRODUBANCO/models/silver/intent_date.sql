WITH ConteoDiario AS (
    SELECT
        DATETIME(CREATED_AT) AS fecha,
        INTENT AS tipo_interaccion,
        COUNT(*) AS total_intenciones
    FROM {{ ref('client_intents') }}
    GROUP BY fecha, tipo_interaccion
),
ClasificacionHoras AS (
    SELECT
        fecha,
        tipo_interaccion,
        total_intenciones,
        CASE 
            WHEN EXTRACT(HOUR FROM fecha) BETWEEN 0 AND 5 THEN 'Madrugada'
            WHEN EXTRACT(HOUR FROM fecha) BETWEEN 6 AND 11 THEN 'Mañana'
            WHEN EXTRACT(HOUR FROM fecha) BETWEEN 12 AND 17 THEN 'Tarde'
            WHEN EXTRACT(HOUR FROM fecha) BETWEEN 18 AND 23 THEN 'Noche'
        END AS periodo_dia
    FROM ConteoDiario
)
SELECT
    fecha,
    tipo_interaccion,
    total_intenciones,
    periodo_dia
FROM ClasificacionHoras
ORDER BY fecha, total_intenciones DESC
