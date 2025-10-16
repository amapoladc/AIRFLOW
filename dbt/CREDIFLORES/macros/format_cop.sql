{% macro format_cop(expr) %}
-- Devuelve STRING en formato internacional: #,###,###.##
REPLACE(                                      -- paso 3: @  →  .
  REPLACE(                                    -- paso 2: ’ (coma)  →  , (coma)
    REGEXP_REPLACE(                           -- paso 1: .dec → @dec
      FORMAT("%'.2f", SAFE_CAST({{ expr }} AS NUMERIC)),   -- miles con ’ flag
      r'\.(\d{2})$', r'@\1'                   -- marca el separador decimal
    ),
    "'", ","                                  -- pone coma de miles
  ),
  '@', '.'                                    -- pone punto decimal
)
{% endmacro %}
