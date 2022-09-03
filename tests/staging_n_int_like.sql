{% set cols = dbtplyr.get_column_names( ref('model_monitor_staging') ) %}
{% set cols_n = dbtplyr.starts_with('n_', cols) %}

select *   
from {{ ref('model_monitor_staging') }}
where
  {{ dbtplyr.if_any(cols_n, "abs( {{var}} - cast({{var}} as int64) ) > 0.01") }} or 
  FALSE