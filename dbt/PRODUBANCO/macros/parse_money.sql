{% macro parse_money(value) %}
    COALESCE(
        CASE
            -- Directly cast if it's already a valid FLOAT64
            WHEN SAFE_CAST({{ value }} AS FLOAT64) IS NOT NULL THEN ROUND(SAFE_CAST({{ value }} AS FLOAT64), 2)

            -- If the value is a STRING, clean it and cast it to FLOAT64
            WHEN SAFE_CAST({{ value }} AS STRING) IS NOT NULL 
                THEN ROUND(SAFE_CAST(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(CAST({{ value }} AS STRING), r'\\.', ''), r',', '.'
                    ) AS FLOAT64
                ), 2)

            -- Remove common currency symbols ($, €, £)
            WHEN REGEXP_CONTAINS(CAST({{ value }} AS STRING), r'[$€£]') THEN 
                ROUND(SAFE_CAST(
                    REPLACE(REPLACE(REPLACE(CAST({{ value }} AS STRING), '$', ''), '€', ''), '£', '') 
                    AS FLOAT64
                ), 2)

            -- Handle thousands separator with comma and dot for decimals (e.g., "1,000.50" → "1000.50")
            WHEN REGEXP_CONTAINS(CAST({{ value }} AS STRING), r'^[0-9]{1,3}(,[0-9]{3})*\\.[0-9]+$') THEN 
                ROUND(SAFE_CAST(REPLACE(CAST({{ value }} AS STRING), ',', '') AS FLOAT64), 2)

            -- Handle European format (e.g., "1.000,50" → "1000.50")
            WHEN REGEXP_CONTAINS(CAST({{ value }} AS STRING), r'^[0-9]{1,3}(\\.[0-9]{3})*,[0-9]+$') THEN 
                ROUND(SAFE_CAST(
                    REPLACE(REPLACE(CAST({{ value }} AS STRING), '\\.', ''), ',', '.') AS FLOAT64
                ), 2)

            ELSE NULL
        END, 
        0.00  -- Replace NULL with 0.00
    )
{% endmacro %}
