{{ config(materialized='table') }}

with base as (

    select distinct
        case
            when payment_type is null then 'unknown'
            when lower(trim(payment_type::text)) in ('1', 'credit card') then 'card'
            when lower(trim(payment_type::text)) in ('2', 'cash') then 'cash'
            when lower(trim(payment_type::text)) in ('3', 'no charge') then 'no_charge'
            when lower(trim(payment_type::text)) in ('4', 'dispute') then 'dispute'
            when lower(trim(payment_type::text)) in ('5', 'unknown') then 'unknown'
            when lower(trim(payment_type::text)) in ('6', 'voided trip') then 'voided_trip'
            else 'other'
        end as payment_type
    from {{ ref('silver_trips_enriched') }}

),

final as (

    select
        md5(payment_type) as payment_type_key,
        payment_type
    from base
)

select *
from final