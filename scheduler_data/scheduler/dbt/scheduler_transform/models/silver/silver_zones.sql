{{ config(materialized='view') }}

select
    cast(locationid as int) as location_id,
    borough,
    zone,
    service_zone
from {{ source('bronze', 'taxi_zones') }}