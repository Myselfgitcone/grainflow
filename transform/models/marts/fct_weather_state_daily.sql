select
    cast(state_alpha as varchar) as state_alpha,
    cast(obs_date as date) as obs_date,
    cast(avg(tmin_f) as double) as tmin_f,
    cast(avg(tmax_f) as double) as tmax_f,
    cast(avg(gdd_f) as double) as gdd_f,
    cast(avg(precip_mm) as double) as precip_mm,
    cast(count(*) as bigint) as station_count
from {{ ref('int_weather_station_daily') }}
where is_complete_day
group by state_alpha, obs_date
