{{ config(materialized='table') }}

with trips as (

    select *
    from {{ ref('silver_trips_enriched') }}

),

final as (

    select
        md5(
            concat_ws('|',
                coalesce(lower(trim(t.service_type)), ''),
                coalesce(t.source_month, ''),
                coalesce(t.pickup_at::text, ''),
                coalesce(t.dropoff_at::text, ''),
                coalesce(t.pu_location_id::text, ''),
                coalesce(t.do_location_id::text, ''),
                coalesce(t.vendor_id::text, ''),
                coalesce(t.payment_type::text, ''),
                coalesce(t.passenger_count::text, ''),
                coalesce(t.trip_distance::text, ''),
                coalesce(t.fare_amount::text, ''),
                coalesce(t.tip_amount::text, ''),
                coalesce(t.total_amount::text, '')
            )
        ) as trip_fact_key,

        dst.service_type_key,
        dpt.payment_type_key,
        dv.vendor_key,
        dd.date_key as pickup_date_key,
        dz_pu.zone_key as pu_location_key,
        dz_do.zone_key as do_location_key,

        t.source_month,
        t.ingest_ts,
        t.pickup_at,
        t.dropoff_at,
        t.passenger_count,
        t.trip_distance,

        case
            when t.pickup_at is not null
             and t.dropoff_at is not null
            then extract(epoch from (t.dropoff_at - t.pickup_at)) / 60.0
            else null
        end as trip_duration_min,

        t.fare_amount,
        t.extra,
        t.mta_tax,
        t.tip_amount,
        t.tolls_amount,
        t.total_amount

    from trips t

    left join {{ ref('dim_service_type') }} dst
        on dst.service_type = lower(trim(t.service_type))

    left join {{ ref('dim_payment_type') }} dpt
        on dpt.payment_type =
            case
                when t.payment_type is null then 'unknown'
                when lower(trim(t.payment_type::text)) in ('1', 'credit card') then 'card'
                when lower(trim(t.payment_type::text)) in ('2', 'cash') then 'cash'
                when lower(trim(t.payment_type::text)) in ('3', 'no charge') then 'no_charge'
                when lower(trim(t.payment_type::text)) in ('4', 'dispute') then 'dispute'
                when lower(trim(t.payment_type::text)) in ('5', 'unknown') then 'unknown'
                when lower(trim(t.payment_type::text)) in ('6', 'voided trip') then 'voided_trip'
                else 'other'
            end

    left join {{ ref('dim_vendor') }} dv
        on dv.vendor_id = coalesce(t.vendor_id::text, 'unknown')

    left join {{ ref('dim_date') }} dd
        on dd.date_day = date(t.pickup_at)

    left join {{ ref('dim_zone') }} dz_pu
        on dz_pu.location_id = t.pu_location_id

    left join {{ ref('dim_zone') }} dz_do
        on dz_do.location_id = t.do_location_id
)

select *
from final