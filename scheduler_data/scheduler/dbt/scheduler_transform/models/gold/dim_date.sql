{{ config(materialized='table') }}

with dates as (
    select distinct
        date_trunc('day', pickup_at)::date as date_day
    from {{ ref('silver_trips_enriched') }}
    where pickup_at is not null
),

final as (
    select
        to_char(date_day, 'YYYYMMDD')::int as date_key,
        date_day,
        extract(year from date_day)::int as year,
        extract(month from date_day)::int as month,
        extract(day from date_day)::int as day,
        extract(dow from date_day)::int as day_of_week
    from dates
)

select *
from final
order by date_key