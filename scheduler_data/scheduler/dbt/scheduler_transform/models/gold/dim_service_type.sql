{{ config(
    materialized='table'
) }}

with service_types as (

    select distinct
        trim(lower(service_type)) as service_type
    from {{ ref('silver_trips') }}
    where service_type is not null
      and trim(service_type) <> ''

),

final as (

    select
        row_number() over (order by service_type) as service_type_key,
        service_type
    from service_types

)

select *
from final
order by service_type_key