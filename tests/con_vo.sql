with cols as (
select 
  column_name, 
  regexp_extract(lower(column_name), '^[a-z]+') as l1,
  regexp_extract(lower(column_name), '^[a-z]+_([a-z]+)') as l2,
  regexp_extract(lower(column_name), '^[a-z]+_[a-z]+_([a-z]+)') as l3
from 
  {{ ref('model_monitor').database }}.
    {{ ref('model_monitor').schema }}.
	  INFORMATION_SCHEMA.COLUMNS
where table_name = '{{ ref('model_monitor').identifier }}'
)

select *
from cols 
where 
  l1 not in ('id', 'cd', 'n', 'nm', 'prop', 'pct', 'dt', 'ind') or 
  l2 not in ('county', 'state', 'case', 'hosp', 'death') or 
  l3 not in ('hpsa','pred', 'actl')