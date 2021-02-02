{{
    config(
        materialized='incremental',
        unique_key= 'id'
    )
}}

select
  county_fips_code || ' ' || forecast_date as id,
  county_fips_code as cd_county,
  forecast_date as dt_county,
  {% for l in var('lags') %}
    max(if(date_diff(prediction_date, forecast_date, day) = {{l}}, 
         round(100*new_confirmed, 0), null)) as n_case_pred_{{l}},
    max(if(date_diff(prediction_date, forecast_date, day) = {{l}}, 
         round(100*hospitalized_patients, 0), null)) as n_hosp_pred_{{l}},
    max(if(date_diff(prediction_date, forecast_date, day) = {{l}}, 
         round(100*new_deaths, 0), null)) as n_death_pred_{{l}}
  {% if not loop.last %},{% endif %}
  {% endfor %}
from {{ source('bqpred', 'pred') }}
where 
  cast(left(county_fips_code, 2) as int64) between 1 and 56 and
  forecast_date <= current_date()
  {% if is_incremental() %}
  and forecast_date >= (
    select dateadd(day, -7, max(dt_county)) from {{this}}
  )
  {% endif %}
group by 1,2,3
