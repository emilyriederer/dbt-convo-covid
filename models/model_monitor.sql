{{
    config(
        materialized='view',
        unique_key='id',
		partition_by={
		  "field": "dt_county",
		  "data_type": "date",
		  "granularity": "month"
		}
    )
}}

{% set cols = dbtplyr.get_column_names( ref('model_monitor_staging') ) %}
{% set cols_n = dbtplyr.starts_with('n', cols) %}
{% set cols_dt = dbtplyr.starts_with('dt', cols) %}
{% set cols_prop = dbtplyr.starts_with('prop', cols) %}
{% set cols_ind = dbtplyr.starts_with('ind', cols) %}
{% set cols_class = cols_n + cols_dt + cols_prop + cols_ind %}
{% set cols_oth = dbtplyr.not_one_of(cols_class, cols) %}

select
	
  {{ dbtplyr.across(cols_oth) }},
  {{ dbtplyr.across(cols_n, "cast({{var}} as int64) as {{var}}") }},
  {{ dbtplyr.across(cols_dt, "date({{var}}) as {{var}}")}},
  {{ dbtplyr.across(cols_prop, "round({{var}}, 3) as {{var}}")}},
  {{ dbtplyr.across(cols_ind, "coalesce({{var}}, 0) as {{var}}")}}
   
from {{ ref('model_monitor_staging') }}

{% if is_incremental() %}
where dt_county >= (
  select dateadd(day, -7, max(dt_county)) from {{this}}
  )
{% endif %}


  
