select

  {%- for l in var('lags') %}
    {%- for m in ['case', 'death'] %}
	  case 
	    when n_{{m}}_actl = 0 then null 
        else round( (n_{{m}}_actl - n_{{m}}_pred_{{l}}) / n_{{m}}_actl, 4)
      end as pctdiff_{{m}}_pred_{{l}} ,  
    {% endfor %}
  {% endfor %}
  
  mm.*
  
from {{ ref('model_monitor') }} as mm