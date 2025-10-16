

-- tests/phone_all_in_one_test.sql
{{ config(severity = 'error') }}   -- FAILED si la SELECT final devuelve ≥1 fila

/* 1️⃣  Fuentes normalizadas */
WITH clean AS (
    SELECT
        CAST(tarjeta_dig AS STRING)           AS tarjeta_key,
        CAST(cedula      AS STRING)           AS cedula_clean,
        CAST(celular     AS STRING)           AS celular_clean
    FROM `charlieserver-281513.crediflores_prod.DATA_WAREHAUSE_CLEAN`      -- modelo o tabla histórica
),
cobranza AS (
    SELECT
        CAST(TARJETA_DIG AS STRING)           AS tarjeta_key,
        CAST(CEDULA      AS STRING)           AS cedula_cobranza,
        -- quita prefijo 57 en celulares de cobranza
        CASE
          WHEN LEFT(CELULAR,2) = '57' THEN SUBSTR(CELULAR,3)
          ELSE CELULAR
        END                                   AS celular_cobranza
    FROM `charlieserver-281513.crediflores_prod.DATA_WAREHAUSE_BL_COBRANZA`  -- tabla diaria
),

/* 2️⃣  Cálculo de estados */
diff AS (
    SELECT
        COALESCE(cobranza.tarjeta_key, clean.tarjeta_key) AS tarjeta_dig,
        clean.cedula_clean,
        cobranza.cedula_cobranza,
        clean.celular_clean,
        cobranza.celular_cobranza,

        /* Estado cédula */
        CASE
          WHEN clean.cedula_clean = cobranza.cedula_cobranza THEN 'MATCH'
          ELSE 'NO_MATCH'
        END AS estado_cedula,

        /* Estado celular */
        CASE
          -- 1️⃣ Cédula match + celular de cobranza nulo/ vacío
          WHEN clean.cedula_clean = cobranza.cedula_cobranza
           AND (cobranza.celular_cobranza IS NULL
                OR TRIM(cobranza.celular_cobranza) = '')
            THEN 'NO_DATOS_ACORDADOS'

          -- 2️⃣ Cédula match + celular distinto
          WHEN clean.cedula_clean = cobranza.cedula_cobranza
           AND clean.celular_clean IS DISTINCT FROM cobranza.celular_cobranza
            THEN 'CAMBIO'

          -- 3️⃣ Coinciden cédula y celular
          WHEN clean.cedula_clean = cobranza.cedula_cobranza
            THEN 'MATCH'

          -- 4️⃣ Solo existe en histórico
          WHEN cobranza.cedula_cobranza IS NULL
            THEN 'SOLO_CLEAN'

          -- 5️⃣ Solo existe en diaria
          WHEN clean.cedula_clean IS NULL
            THEN 'SOLO_DIARIA'

          ELSE 'OTRO'
        END AS estado_celular
    FROM clean
    FULL OUTER JOIN cobranza USING (tarjeta_key)
)

/* 3️⃣  Condición de fallo del test */
SELECT *
FROM diff
WHERE estado_celular IN ( 'CAMBIO')

