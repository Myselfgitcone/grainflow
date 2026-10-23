{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key=['symbol', 'bar_interval', 'bar_start'],
    )
}}

{#
  Bronze is at-least-once: a restarted stream or a replay can land the same
  bar twice. Keep the most recently ingested copy of each bar. On incremental
  runs, look back an hour so late duplicates still collapse onto one row.
#}

with source as (

    select * from {{ source('bronze', 'price_bars') }}

    {% if is_incremental() %}
    where _ingested_at > (select max(ingested_at) from {{ this }}) - interval '1' hour
    {% endif %}

),

ranked as (

    select
        *,
        row_number() over (
            partition by symbol, bar_interval, bar_start
            order by _ingested_at desc, _kafka_offset desc
        ) as copy_rank
    from source

)

select
    symbol,
    bar_interval,
    bar_start,
    open as open_price,
    high as high_price,
    low as low_price,
    close as close_price,
    volume,
    source as feed,
    _ingested_at as ingested_at
from ranked
where copy_rank = 1
