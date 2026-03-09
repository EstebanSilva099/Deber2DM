{{ config(materialized='view') }}

with trips as (
    select * from {{ ref('silver_trips') }}
),
zones as (
    select * from {{ ref('silver_zones') }}
)

select
    t.*,
    pu.borough as pu_borough,
    pu.zone as pu_zone,
    pu.service_zone as pu_service_zone,
    doo.borough as do_borough,
    doo.zone as do_zone,
    doo.service_zone as do_service_zone
from trips t
left join zones pu
  on t.pu_location_id = pu.location_id
left join zones doo
  on t.do_location_id = doo.location_id