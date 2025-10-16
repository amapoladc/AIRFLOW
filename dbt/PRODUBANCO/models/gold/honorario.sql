
WITH HonorarioLimits AS (
    SELECT 1 AS Dias_de_Mora_Min, 15 AS Dias_de_Mora_Max, 0.37 AS Honorario_Min, 0.37 AS Honorario_Max UNION ALL
    SELECT 16, 30, 3.11, 7.92 UNION ALL
    SELECT 31, 60, 5.18, 17.83 UNION ALL
    SELECT 61, 90, 6.91, 25.77 UNION ALL
    SELECT 91, 120, 8.64, 29.03 UNION ALL
    SELECT 121, 180, 8.64, 29.03 UNION ALL
    SELECT 181, 270, 8.64, 29.03 UNION ALL
    SELECT 271, 9999, 8.64, 29.03  
),
-- Calculate the Fee_Percentage based on TIPO_CAMBIO, Dias_de_Mora, and MONTO_PAGO
Calculations AS (
    SELECT 
        p.*,
        CASE
            WHEN TIPO_CAMBIO = 'PAGO_INFERIDO' THEN
                CASE
                    WHEN DIAS_MORA BETWEEN 1 AND 15 THEN 0.00
                    WHEN DIAS_MORA BETWEEN 16 AND 30 THEN 
                        CASE 
                            WHEN MONTO_PAGO BETWEEN 20 AND 599.99 THEN 1.61
                            WHEN MONTO_PAGO BETWEEN 600 AND 1499.99 THEN 1.61
                            WHEN MONTO_PAGO > 1500 THEN 1.71
                            ELSE 0
                        END
                    WHEN DIAS_MORA BETWEEN 31 AND 60 THEN 
                        CASE 
                            WHEN MONTO_PAGO BETWEEN 20 AND 599.99 THEN 2.59
                            WHEN MONTO_PAGO BETWEEN 600 AND 1499.99 THEN 2.59
                            WHEN MONTO_PAGO > 1500 THEN 1.73
                            ELSE 0
                        END
                    WHEN DIAS_MORA BETWEEN 61 AND 90 THEN 
                        CASE 
                            WHEN MONTO_PAGO BETWEEN 20 AND 599.99 THEN 4.32
                            WHEN MONTO_PAGO BETWEEN 600 AND 1499.99 THEN 4.32
                            WHEN MONTO_PAGO > 1500 THEN 2.59
                            ELSE 0
                        END
                    WHEN DIAS_MORA BETWEEN 91 AND 120 THEN 
                        CASE 
                            WHEN MONTO_PAGO BETWEEN 20 AND 599.99 THEN 6.91
                            WHEN MONTO_PAGO BETWEEN 600 AND 1499.99 THEN 6.91
                            WHEN MONTO_PAGO > 1500 THEN 5.18
                            ELSE 0
                        END
                    WHEN DIAS_MORA BETWEEN 121 AND 180 THEN
                        CASE
                            WHEN MONTO_PAGO BETWEEN 20 AND 599.99 THEN 6.91
                            WHEN MONTO_PAGO BETWEEN 600 AND 1499.99 THEN 6.91
                            WHEN MONTO_PAGO > 1500 THEN 5.18
                            ELSE 0
                        END
                    WHEN DIAS_MORA BETWEEN 181 AND 270 THEN 
                        CASE 
                            WHEN MONTO_PAGO BETWEEN 20 AND 599.99 THEN 9.07
                            WHEN MONTO_PAGO BETWEEN 600 AND 1499.99 THEN 9.07
                            WHEN MONTO_PAGO > 1500 THEN 8.21
                            ELSE 0
                        END
                    WHEN DIAS_MORA > 270 THEN 
                        CASE 
                            WHEN MONTO_PAGO BETWEEN 20 AND 599.99 THEN 9.07
                            WHEN MONTO_PAGO BETWEEN 600 AND 1499.99 THEN 9.07
                            WHEN MONTO_PAGO > 1500 THEN 8.21
                            ELSE 0
                        END
                    ELSE 0
                END
            ELSE
                0
        END AS Fee_Percentage
    FROM {{ ref('diferencias') }} p
),
-- Calculate the Percentage_Charge
WithCharges AS (
    SELECT
        c.*,
        CASE 
            WHEN (MONTO_PAGO * Fee_Percentage) / 100 = 0 THEN 0.37 
            ELSE (MONTO_PAGO * Fee_Percentage) / 100 
        END AS Percentage_Charge
    FROM Calculations c
)
-- Final calculation of Honorario
SELECT 
    wc.*,
    CASE
        -- Apply Minimum Honorario if Percentage_Charge is less than Honorario_Min
        WHEN wc.Percentage_Charge < hl.Honorario_Min THEN hl.Honorario_Min
        -- Apply Maximum Honorario if Percentage_Charge is greater than Honorario_Max
        WHEN wc.Percentage_Charge > hl.Honorario_Max THEN hl.Honorario_Max
        -- Use Percentage_Charge directly if within bounds
        ELSE wc.Percentage_Charge
    END AS Honorario
FROM WithCharges wc
LEFT JOIN HonorarioLimits hl
    ON wc.DIAS_MORA BETWEEN hl.Dias_de_Mora_Min AND hl.Dias_de_Mora_Max

WHERE  wc.TIPO_CAMBIO = 'PAGO_INFERIDO'