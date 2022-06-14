select
  {{ ref('alternative_catalog') }}.id,
  {{ ref('alternative_catalog') }}.name,
  {{ ref('alternative_catalog') }}.date
from
  {{ ref('alternative_catalog') }}
    inner join {{ ref('refer_alternative_catalog')}} using (id)
