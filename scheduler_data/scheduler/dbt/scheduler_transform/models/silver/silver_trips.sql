{{ config(materialized='view') }}

with base as (

    select
        service_type,
        source_month,
        ingest_ts,

        vendorid,
        tpep_pickup_datetime,
        tpep_dropoff_datetime,
        lpep_pickup_datetime,
        lpep_dropoff_datetime,
        pickup_datetime,
        dropoff_datetime,

        passenger_count,
        trip_distance,
        ratecodeid,
        store_and_fwd_flag,
        pulocationid,
        dolocationid,
        payment_type,

        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        total_amount,
        congestion_surcharge,
        airport_fee,
        ehail_fee,
        trip_type
    from {{ source('bronze', 'taxi_trips') }}

),

standardized as (

    select
        service_type,
        source_month,
        ingest_ts,

        cast(vendorid as int) as vendor_id,

        coalesce(pickup_datetime, tpep_pickup_datetime, lpep_pickup_datetime) as pickup_at,
        coalesce(dropoff_datetime, tpep_dropoff_datetime, lpep_dropoff_datetime) as dropoff_at,

        cast(passenger_count as numeric) as passenger_count,
        cast(trip_distance as numeric) as trip_distance,
        cast(ratecodeid as numeric) as rate_code_id,
        store_and_fwd_flag,
        cast(pulocationid as int) as pu_location_id,
        cast(dolocationid as int) as do_location_id,
        cast(payment_type as numeric) as payment_type,

        cast(fare_amount as numeric) as fare_amount,
        cast(extra as numeric) as extra,
        cast(mta_tax as numeric) as mta_tax,
        cast(tip_amount as numeric) as tip_amount,
        cast(tolls_amount as numeric) as tolls_amount,
        cast(improvement_surcharge as numeric) as improvement_surcharge,
        cast(total_amount as numeric) as total_amount,
        cast(congestion_surcharge as numeric) as congestion_surcharge,
        cast(airport_fee as numeric) as airport_fee,
        cast(ehail_fee as numeric) as ehail_fee,
        cast(trip_type as numeric) as trip_type
    from base

),

cleaned as (

    select
        *,
        extract(epoch from (dropoff_at - pickup_at))/60.0 as trip_duration_min
    from standardized
    where pickup_at is not null
      and dropoff_at is not null
      and dropoff_at >= pickup_at
      and coalesce(trip_distance, 0) >= 0
      and coalesce(total_amount, 0) >= 0
)

select * from cleaned