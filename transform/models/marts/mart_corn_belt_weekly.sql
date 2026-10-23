{#
  One row per week: corn futures, corn belt heat units and crop condition side
  by side. The question it is built to answer: does a drop in good/excellent
  ratings show up in next week's corn price?

  Weeks start on Monday. NASS weeks end on Sunday, so truncating week_ending
  to the week lands on the Monday of the week the rating describes.
#}

with corn_closes as (

    select
        cast(date_trunc('week', trade_date) as date) as week_start,
        close_price,
        row_number() over (
            partition by date_trunc('week', trade_date) order by trade_date desc
        ) as day_rank
    from {{ ref('fct_price_daily') }}
    where symbol = 'ZC=F'

),

corn_weekly as (

    select
        week_start,
        close_price as corn_close,
        close_price / lag(close_price) over (order by week_start) - 1 as corn_return
    from corn_closes
    where day_rank = 1

),

weather_weekly as (

    select
        week_start,
        avg(state_gdd) as avg_state_gdd,
        avg(state_precip_mm) as avg_state_precip_mm
    from (
        select
            cast(date_trunc('week', obs_date) as date) as week_start,
            state_alpha,
            sum(gdd_f) as state_gdd,
            sum(precip_mm) as state_precip_mm
        from {{ ref('fct_weather_state_daily') }}
        group by cast(date_trunc('week', obs_date) as date), state_alpha
    ) as by_state
    group by week_start

),

condition_weekly as (

    select
        cast(date_trunc('week', conditions.week_ending) as date) as week_start,
        avg(conditions.pct_good_excellent) as avg_pct_good_excellent,
        avg(conditions.good_excellent_change) as avg_good_excellent_change,
        count(*) as states_reporting
    from {{ ref('fct_crop_condition_weekly') }} as conditions
    -- Only the major producing states; minor states would dilute the signal.
    inner join {{ ref('ref_crop_progress_states') }} as major
        on conditions.commodity = major.commodity
        and conditions.state_alpha = major.state_alpha
    where conditions.commodity = 'CORN'
    group by cast(date_trunc('week', conditions.week_ending) as date)

)

select
    cast(prices.week_start as date) as week_start,
    cast(prices.corn_close as double) as corn_close,
    cast(prices.corn_return as double) as corn_return,
    cast(lead(prices.corn_return) over (order by prices.week_start) as double)
        as next_week_corn_return,
    cast(weather.avg_state_gdd as double) as gdd_f,
    cast(weather.avg_state_precip_mm as double) as precip_mm,
    cast(conditions.avg_pct_good_excellent as double) as pct_good_excellent,
    cast(conditions.avg_good_excellent_change as double) as good_excellent_change,
    cast(coalesce(conditions.states_reporting, 0) as bigint) as states_reporting
from corn_weekly as prices
left join weather_weekly as weather
    on prices.week_start = weather.week_start
left join condition_weekly as conditions
    on prices.week_start = conditions.week_start
