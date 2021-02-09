with cols_type as (
select distinct 
  regexp_extract(lower(column_name), '^[a-z]+') as stub,
  data_type
from 
  {{ ref('model_monitor').database }}.
    {{ ref('model_monitor').schema }}.
	  INFORMATION_SCHEMA.COLUMNS
where table_name = '{{ ref('model_monitor').identifier }}'
)

select * 
from cols_type
where 
    (stub in ('id', 'cd', 'nm') and not data_type = 'STRING') or 
    (stub in ('n', 'ind') and not data_type = 'INT64') or 
    (stub in ('prop', 'pct') and not data_type = 'FLOAT64') or
    (stub = 'dt' and not data_type = 'DATE')