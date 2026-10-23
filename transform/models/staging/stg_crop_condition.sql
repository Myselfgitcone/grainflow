{# NASS revises numbers in place; the latest load_time wins. #}

with ranked as (

    select
        *,
        row_number() over (
            partition by commodity, state_alpha, week_ending, condition_class
            order by nass_load_time desc, _ingested_at desc
        ) as copy_rank
    from {{ source('bronze', 'crop_condition') }}

)

select
    commodity,
    state_alpha,
    week_ending,
    lower(replace(condition_class, ' ', '_')) as condition_class,
    pct,
    nass_load_time,
    _ingested_at as ingested_at
from ranked
where copy_rank = 1
