{#
  ASOS stations send a routine report near :54 every hour plus extra
  "special" reports when conditions change. Each report's precipitation
  covers the last hour, so summing every report double counts rain. Take the
  largest reading per station-hour first, then sum the hours.
#}

with hourly as (

    select
        station_id,
        state_alpha,
        date_trunc('hour', observed_at) as observed_hour,
        min(temperature_c) as tmin_c,
        max(temperature_c) as tmax_c,
        max(precipitation_last_hour_mm) as precip_mm
    from {{ ref('stg_weather_observations') }}
    group by station_id, state_alpha, date_trunc('hour', observed_at)

),

daily as (

    select
        station_id,
        state_alpha,
        cast(observed_hour as date) as obs_date,
        min(tmin_c) as tmin_c,
        max(tmax_c) as tmax_c,
        coalesce(sum(precip_mm), 0.0) as precip_mm,
        count(tmin_c) as hours_with_temperature
    from hourly
    group by station_id, state_alpha, cast(observed_hour as date)

)

select
    station_id,
    state_alpha,
    obs_date,
    tmin_c,
    tmax_c,
    {{ celsius_to_fahrenheit('tmin_c') }} as tmin_f,
    {{ celsius_to_fahrenheit('tmax_c') }} as tmax_f,
    {{ corn_gdd(celsius_to_fahrenheit('tmin_c'), celsius_to_fahrenheit('tmax_c')) }} as gdd_f,
    precip_mm,
    hours_with_temperature,
    -- A day with big gaps would understate the max and overstate GDD stability.
    hours_with_temperature >= 18 as is_complete_day
from daily
