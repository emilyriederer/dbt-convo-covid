{% set cols = dbtplyr.get_column_names( ref('model_monitor') ) %}
{% set cols_n = dbtplyr.starts_with('n_', cols) %}

select *   
from {{ ref('model_monitor') }}
where
  {{ dbtplyr.if_any(cols_n, "{{var}} is null") }} or 
  FALSE
  