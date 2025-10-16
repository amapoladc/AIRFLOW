WITH CE_mensaje AS (
    SELECT
        d.CEDULA                AS difusion_cedula,
        d.TEMPLATE              AS difusion_template,
        d.ULTIMO_ESTADO_MSJ     AS difusion_ultimo_estado_msj,
        d.UPDATED_AT            AS difusion_updated_at,
        ci.INTENT               AS client_intent,
        ci.CREATED_AT           AS client_created_at,
        CASE 
            WHEN ci.INTENT IN ('detalle_saldo_val', 'detalle_saldo') THEN 'INTECION'
            WHEN d.TEMPLATE IN (
                'rbc_primer_contacto_61_90',
                'rbc_primer_mensaje_241_360',
                'rbc_primer_mensaje_121_240',
                'rbc_primer_contacto_91_120',
                'rbc_tercer_contacto_31_60',
                'rbc_segundo_contacto_6_30',
                'rbc_primer_contacto_1_5',
                'rbc_compromiso_pago',
                'rbc_dias'
            )
            AND d.ULTIMO_ESTADO_MSJ = 'read' THEN 'TEMPLATE'
            WHEN CAST(ci.CREATED_AT AS DATETIME) <= CAST(d.UPDATED_AT AS DATETIME)
            AND d.ULTIMO_ESTADO_MSJ NOT IN ('read','failed') THEN 'TEMPLATE_INTECION'
            ELSE NULL
        END AS TIPO_CE
    FROM `charlieserver-281513.produbanco_dev.difusion` AS d
    JOIN `charlieserver-281513.produbanco_dev.client_intents` AS ci
        ON d.CEDULA = ci.CEDULA
),

CE_mensaje_unique AS (
    SELECT
      difusion_cedula,
      difusion_template,
      difusion_ultimo_estado_msj,
      difusion_updated_at,
      client_intent,
      client_created_at,
      TIPO_CE,
      ROW_NUMBER() OVER(
        PARTITION BY difusion_cedula 
        ORDER BY difusion_updated_at DESC
      ) AS rn
    FROM CE_mensaje
)

-- Final SELECT for unique CEDULA row
SELECT
  difusion_cedula,
  difusion_template,
  difusion_ultimo_estado_msj,
  difusion_updated_at,
  client_intent,
  client_created_at,
  TIPO_CE
FROM CE_mensaje_unique
WHERE rn = 1;
