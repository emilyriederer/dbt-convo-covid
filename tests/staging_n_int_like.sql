{% set cols = get_column_names( ref('model_monitor_staging') ) %}
{% set cols_n = get_matches(cols, '^n_.*') %}

select *   
from {{ ref('model_monitor_staging') }}
where
   {%- for c in cols_n %} abs({{c}} - cast({{c}} as int64)) > 0.01 or 
   {% endfor %}
   FALSE