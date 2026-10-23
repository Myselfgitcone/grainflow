with pivoted as (

    select
        commodity,
        state_alpha,
        week_ending,
        sum(case when condition_class = 'very_poor' then pct end) as pct_very_poor,
        sum(case when condition_class = 'poor' then pct end) as pct_poor,
        sum(case when condition_class = 'fair' then pct end) as pct_fair,
        sum(case when condition_class = 'good' then pct end) as pct_good,
        sum(case when condition_class = 'excellent' then pct end) as pct_excellent,
        -- 1 = very poor .. 5 = excellent, weighted by share of acres.
        sum(
            pct * case condition_class
                when 'very_poor' then 1
                when 'poor' then 2
                when 'fair' then 3
                when 'good' then 4
                when 'excellent' then 5
            end
        ) / nullif(sum(pct), 0) as condition_index
    from {{ ref('stg_crop_condition') }}
    group by commodity, state_alpha, week_ending

),

with_change as (

    select
        *,
        coalesce(pct_good, 0) + coalesce(pct_excellent, 0) as pct_good_excellent
    from pivoted

)

select
    cast(commodity as varchar) as commodity,
    cast(state_alpha as varchar) as state_alpha,
    cast(week_ending as date) as week_ending,
    cast(pct_very_poor as double) as pct_very_poor,
    cast(pct_poor as double) as pct_poor,
    cast(pct_fair as double) as pct_fair,
    cast(pct_good as double) as pct_good,
    cast(pct_excellent as double) as pct_excellent,
    cast(pct_good_excellent as double) as pct_good_excellent,
    cast(condition_index as double) as condition_index,
    -- Change vs the previous report in the same crop year.
    cast(
        pct_good_excellent - lag(pct_good_excellent) over (
            partition by commodity, state_alpha, extract(year from week_ending)
            order by week_ending
        ) as double
    ) as good_excellent_change
from with_change
