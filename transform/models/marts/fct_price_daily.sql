with daily as (

    select
        bars.symbol,
        symbols.commodity,
        cast(bars.bar_start as date) as trade_date,
        bars.open_price,
        bars.high_price,
        bars.low_price,
        bars.close_price,
        bars.volume
    from {{ ref('stg_price_bars') }} as bars
    inner join {{ ref('ref_symbols') }} as symbols
        on bars.symbol = symbols.symbol
    where bars.bar_interval = '1d'

),

with_returns as (

    select
        *,
        close_price / lag(close_price) over (partition by symbol order by trade_date) - 1
            as daily_return,
        ln(close_price / lag(close_price) over (partition by symbol order by trade_date))
            as log_return
    from daily

),

with_volatility as (

    select
        *,
        stddev_samp(log_return) over trailing_20 as stddev_20d,
        count(log_return) over trailing_20 as returns_in_window
    from with_returns
    window trailing_20 as (
        partition by symbol order by trade_date rows between 19 preceding and current row
    )

)

select
    cast(symbol as varchar) as symbol,
    cast(commodity as varchar) as commodity,
    cast(trade_date as date) as trade_date,
    cast(open_price as double) as open_price,
    cast(high_price as double) as high_price,
    cast(low_price as double) as low_price,
    cast(close_price as double) as close_price,
    cast(volume as bigint) as volume,
    cast(daily_return as double) as daily_return,
    -- Annualised, and only once a full 20-day window exists.
    cast(
        case when returns_in_window = 20 then stddev_20d * sqrt(252) end as double
    ) as realized_vol_20d
from with_volatility
