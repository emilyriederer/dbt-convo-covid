{{
    config(
        materialized='incremental',
        unique_key='id'
    )
}}

select
  actual.*,
  prediction.* except (cd_county, dt_county, id),
  fips.* except (cd_county),
  hspa.* except (cd_county)
from
  {{ ref('actual') }} as actual
  inner join
  {{ ref('prediction') }} as prediction
  using (dt_county, cd_county)
  left join
  {{ ref('fips') }} as fips
  using (cd_county)
  left join
  {{ ref('hpsa') }} as hspa
  using (cd_county)
{% if is_incremental() %}
where dt_county >= (
  select dateadd(day, -7, max(dt_county)) from {{this}}
  )
{% endif %}