select 
  if(length(StateCountyFIPS) = 4, '0', '') || statecountyfips as cd_county, 
  designation_date as dt_county_hpsa, 
  estimated_underserved_pop / designation_pop as prop_county_hpsa,
  1 as ind_county_hpsa
from {{ source('bqhspa', 'hpsa') }}
where 
  type_code = 'Hpsa Geo' and 
  component_type_code = 'SCTY' and
  HPSA_Withdrawn_Date IS NULL and
  cast(state_fips as int64) between 1 and 56
