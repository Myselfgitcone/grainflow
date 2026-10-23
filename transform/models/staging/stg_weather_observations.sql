with ranked as (

    select
        *,
        row_number() over (
            partition by station_id, observed_at
            order by _ingested_at desc, _kafka_offset desc
        ) as copy_rank
    from {{ source('bronze', 'weather_observations') }}

)

select
    station_id,
    state_alpha,
    observed_at,
    temperature_c,
    dewpoint_c,
    relative_humidity_pct,
    wind_speed_kmh,
    precipitation_last_hour_mm,
    description,
    _ingested_at as ingested_at
from ranked
where copy_rank = 1
