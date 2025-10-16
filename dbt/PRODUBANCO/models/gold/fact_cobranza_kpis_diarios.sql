
WITH diario AS (
  SELECT
    f.FECHA_PROCESO,
    
    -- 1) Total de clientes activos del día
    COUNT(DISTINCT f.CEDULA) AS NUMERO_CLIENTES,

    -- 2) Clientes que hicieron interacción/pago
    COUNT(DISTINCT CASE 
                     WHEN EXISTS (
                       SELECT 1 
                       FROM charlieserver-281513.produbanco_dev.prod_clients_intents i
                       WHERE i.CEDULA = f.CEDULA
                         AND DATE(i.CREATED_AT) > f.FECHA_PROCESO
                     )
                     THEN f.CEDULA
                   END) AS CLIENTES_INTERACTUARON,

    -- 3) # de acciones de cobranza
    COUNT(DISTINCT CASE
                     WHEN f.RESPOND = 2
                     THEN f.CEDULA
                   END) AS GESTION_COBRO,

    -- 4) # de inferencias de pago
    COUNT(DISTINCT CASE
                    WHEN f.TIPO_CAMBIO = 'PAGO_INFERIDO'
                    THEN f.CEDULA
                  END) AS INFERENCIAS_PAGO,

    -- 5) Valor de pagos inferidos
    SUM(CASE 
          WHEN TIPO_CAMBIO = 'PAGO_INFERIDO' 
          THEN MONTO_PAGO 
          ELSE 0 
        END) AS VALOR_USD_INFERENCIAS,

    -- 6) Valor de honorarios inferidos
    SUM(CASE 
          WHEN TIPO_CAMBIO = 'PAGO_INFERIDO' 
          THEN Honorario 
          ELSE 0 
        END) AS VALOR_USD_HONORARIOS,

    -- 7) Valor en gestión de cobro (asumo que se usa MONTO_PAGO como valor de gestión)
    SUM(CASE
          WHEN f.RESPOND = 2 
          THEN MONTO_PAGO
          ELSE 0
        END) AS VALOR_USD_GESTION

  FROM charlieserver-281513.produbanco_dev.fact_cobranza_detalle f
  GROUP BY f.FECHA_PROCESO
),

calculado AS (
  SELECT
    FECHA_PROCESO,
    NUMERO_CLIENTES,
    CLIENTES_INTERACTUARON,
    GESTION_COBRO,
    INFERENCIAS_PAGO,
    VALOR_USD_INFERENCIAS,
    VALOR_USD_HONORARIOS,
    VALOR_USD_GESTION,

    -- 8) Porcentajes
    CASE 
      WHEN NUMERO_CLIENTES = 0 THEN 0
      ELSE (CLIENTES_INTERACTUARON * 100.0 / NUMERO_CLIENTES)
    END AS PORC_CLIENTES_INTERACT,

    CASE 
      WHEN NUMERO_CLIENTES = 0 THEN 0
      ELSE (GESTION_COBRO * 100.0 / NUMERO_CLIENTES)
    END AS PORC_GESTION_COBRO,

    CASE 
      WHEN NUMERO_CLIENTES = 0 THEN 0
      ELSE (INFERENCIAS_PAGO * 100.0 / NUMERO_CLIENTES)
    END AS PORC_INFERENCIAS_PAGO
  FROM diario
)

SELECT *
FROM calculado;
