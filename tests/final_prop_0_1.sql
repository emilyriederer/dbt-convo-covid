{% set cols = get_column_names( ref('model_monitor') ) %}
{% set cols_n = get_matches(cols, '^prop_.*') %}

select *   
from {{ ref('model_monitor') }}
where
   {%- for c in cols_n %} ({{c}} < 0 or {{c}} > 1) or 
   {% endfor %}
   FALSE
   