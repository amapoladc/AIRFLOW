{% macro parse_date_safe(column_name) %}
    CASE 
        -- ✅ Ensure it has the correct length and format YYYY-MM-DD
        WHEN LENGTH(CAST({{ column_name }} AS STRING)) = 10 
             AND SAFE.PARSE_DATE('%Y-%m-%d', CAST({{ column_name }} AS STRING)) IS NOT NULL
        THEN PARSE_DATE('%Y-%m-%d', CAST({{ column_name }} AS STRING))

        -- ✅ Handle standard YYYY-MM-DD format
        WHEN SAFE.PARSE_DATE('%Y-%m-%d', CAST({{ column_name }} AS STRING)) IS NOT NULL
        THEN PARSE_DATE('%Y-%m-%d', CAST({{ column_name }} AS STRING))

        -- ✅ Handle DD/MM/YYYY format
        WHEN SAFE.PARSE_DATE('%d/%m/%Y', CAST({{ column_name }} AS STRING)) IS NOT NULL
        THEN PARSE_DATE('%d/%m/%Y', CAST({{ column_name }} AS STRING))

        -- ✅ Handle MM-DD-YYYY format
        WHEN SAFE.PARSE_DATE('%m-%d-%Y', CAST({{ column_name }} AS STRING)) IS NOT NULL
        THEN PARSE_DATE('%m-%d-%Y', CAST({{ column_name }} AS STRING))

        -- ✅ Handle timestamps (if needed)
        WHEN SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', CAST({{ column_name }} AS STRING)) IS NOT NULL
        THEN DATE(SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', CAST({{ column_name }} AS STRING)))

        ELSE NULL -- ❌ If no valid format found, return NULL
    END
{% endmacro %}
