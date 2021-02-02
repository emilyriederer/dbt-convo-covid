{{
    config(
        materialized='incremental',
        unique_key='id'
    )
}}

select 
  date || ' ' || fips as id,
  date as dt_county,
  fips as cd_county,
  confirmed as n_case_actl,
  deaths as n_death_actl,
from {{ source('bqjhu', 'actual') }}
where 
  date >= '2020-11-01' and 
  cast(left(fips, 2) as int64) between 1 and 56 and
  (not fips is null)
{% if is_incremental() %}
and date >= (
  select dateadd(day, -7, max(dt_county)) from {{this}}
  )
{% endif %}