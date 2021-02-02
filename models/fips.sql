select 
  county.county_fips_code as cd_county,
  county.state_fips_code as cd_state,  
  county.area_name as nm_county, 
  state.state_name as nm_state
from
  (select county_fips_code, state_fips_code, area_name
   from {{ source('bqcensus', 'fips') }}
   where summary_level_name = 'state-county'
  ) as county
left join
  (select state_fips_code, area_name as state_name
   from {{ source('bqcensus', 'fips') }}
   where summary_level_name = 'state'
  ) as state
on county.state_fips_code = state.state_fips_code
