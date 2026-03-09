{{ config(materialized='table') }}

with base as (

    select distinct
        coalesce(vendor_id::text, 'unknown') as vendor_id
    from {{ ref('silver_trips_enriched') }}

),

final as (

    select
        md5(vendor_id) as vendor_key,
        vendor_id
    from base
)

select *
from final