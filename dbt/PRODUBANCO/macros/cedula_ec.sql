{% macro format_ID_EC(column_name) %}
    case 
        when length(cast({{ column_name }} as string)) = 9 
        then '0' || cast({{ column_name }} as string)
        else cast({{ column_name }} as string)
    end
{% endmacro %}
