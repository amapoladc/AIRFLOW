WITH client_intents AS (
    SELECT
        CAST(id AS STRING) AS ID,
        CAST(id_intent AS STRING) AS ID_INTENT, 
        CAST(id_cliente AS STRING) AS ID_CLIENTE, 
        CAST(created_at AS DATETIME) AS CREATED_AT,
        CAST(updated_at AS DATETIME) AS UPDATED_AT
    FROM {{ source('raw_data', 'cred_clients_intents') }}
)

SELECT 
    ID, 
    ID_INTENT,
    ID_CLIENTE, 
    CREATED_AT,
    UPDATED_AT,
    EXTRACT(YEAR FROM CREATED_AT) AS YEAR,
    EXTRACT(HOUR FROM CREATED_AT) AS HOUR,
    EXTRACT(MINUTE FROM CREATED_AT) AS MINUTE
FROM client_intents
