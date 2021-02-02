{% set cols = get_column_names( ref('model_monitor') ) %}
{% set cols_n = get_matches(cols, '^n_.*') %}

select *   
from {{ ref('model_monitor') }}
where
   {%- for c in cols_n %} ({{c}} is null) or
   {% endfor %}
   FALSE


  
