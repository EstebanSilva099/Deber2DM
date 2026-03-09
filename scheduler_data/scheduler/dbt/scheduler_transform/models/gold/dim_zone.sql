{{ config(materialized='table') }}

with base as (

    select distinct
        location_id,
        borough,
        zone,
        service_zone
    from {{ ref('silver_zones') }}

),

final as (

    select
        md5(location_id::text) as zone_key,
        location_id,
        borough,
        zone,
        service_zone
    from base
)

select *
from final