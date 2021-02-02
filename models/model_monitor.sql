{{
    config(
        materialized='incremental',
        unique_key='id',
		partition_by={
		  "field": "dt_county",
		  "data_type": "date",
		  "granularity": "month"
		}
    )
}}

{% set cols = get_column_names( ref('model_monitor_staging') ) %}
{% set cols_n = get_matches(cols, '^n_.*') %}
{% set cols_dt = get_matches(cols, '^dt_.*') %}
{% set cols_prop = get_matches(cols, '^prop_.*') %}
{% set cols_ind = get_matches(cols, '^ind_.*') %}
{% set cols_oth = cols
   | reject('in', cols_n)
   | reject('in', cols_dt)
   | reject('in', cols_prop)
   | reject('in', cols_ind) %}

select
	
   {%- for c in cols_oth %}
   {{c}},
   {% endfor -%}
   {%- for c in cols_n %} 
     cast({{c}} as int64) as {{c}}, 
   {% endfor %}
   {%- for c in cols_dt %} 
     date({{c}}) as {{c}}, 
   {% endfor -%}
   {%- for c in cols_prop %} 
     round({{c}}, 3) as {{c}}, 
   {% endfor -%}
   {%- for c in cols_ind %} 
     coalesce({{c}}, 0) as {{c}} 
     {% if not loop.last %},{% endif %} 
   {% endfor -%}
   
from {{ ref('model_monitor_staging') }}

{% if is_incremental() %}
where dt_county >= (
  select dateadd(day, -7, max(dt_county)) from {{this}}
  )
{% endif %}


  
