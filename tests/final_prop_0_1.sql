{% set cols = dbtplyr.get_column_names( ref('model_monitor') ) %}
{% set cols_prop = dbtplyr.starts_with('prop_', cols) %}

select *   
from {{ ref('model_monitor') }}
where
  {{ dbtplyr.if_any(cols_prop, "{{var}} < 0 or {{var}} > 1") }} or 
  FALSE