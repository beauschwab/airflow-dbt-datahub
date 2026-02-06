{{
  config(
    materialized='incremental',
    file_format='iceberg',
    unique_key='forecast_id',
    incremental_strategy='merge',
    partition_by=['partition_date'],
    tags=['marts']
  )
}}

/*
  mart_cashflow_forecast.sql
  --------------------------
  Mart model: 30-day rolling cashflow forecast per portfolio.
  Projects expected cash inflows and outflows based on position maturities,
  coupon schedules, and estimated liquidation proceeds.

  Grain: one row per (portfolio_id, forecast_date, partition_date)
*/

with positions as (
    select * from {{ ref('mart_positions') }}
    {% if is_incremental() %}
    where partition_date = '{{ var("partition_date") }}'
    {% endif %}
),

market as (
    select * from {{ ref('stg_market_data') }}
    {% if is_incremental() %}
    where partition_date = '{{ var("partition_date") }}'
    {% endif %}
),

-- Generate a 30-day forecast horizon from the partition date
forecast_dates as (
    select
        explode(
            sequence(
                cast('{{ var("partition_date") }}' as date),
                date_add(cast('{{ var("partition_date") }}' as date), 29)
            )
        ) as forecast_date
),

-- Aggregate position values per portfolio
portfolio_summary as (
    select
        p.portfolio_id,
        sum(p.position_qty * m.close_price) as total_market_value,
        sum(case when m.yield_rate > 0
            then p.notional_value * m.yield_rate / 365
            else 0
        end) as daily_coupon_income,
        sum(case when p.position_qty < 0
            then abs(p.notional_value) * 0.001  -- Estimated daily margin cost
            else 0
        end) as daily_margin_cost,
        p.partition_date
    from positions p
    left join market m
        on p.instrument_id = m.instrument_id
        and p.partition_date = m.partition_date
    group by p.portfolio_id, p.partition_date
),

-- Cross join portfolios with forecast dates and compute projections
forecasted as (
    select
        ps.portfolio_id,
        fd.forecast_date,
        -- Projected inflows: coupon income + maturity proceeds (simplified)
        round(ps.daily_coupon_income, 2) as projected_inflow,
        -- Projected outflows: margin costs + settlement obligations (simplified)
        round(ps.daily_margin_cost, 2) as projected_outflow,
        -- Net cashflow
        round(ps.daily_coupon_income - ps.daily_margin_cost, 2) as net_cashflow,
        ps.partition_date
    from portfolio_summary ps
    cross join forecast_dates fd
)

select
    {{ dbt_utils.generate_surrogate_key([
        'portfolio_id', 'forecast_date', 'partition_date'
    ]) }} as forecast_id,
    portfolio_id,
    forecast_date,
    projected_inflow,
    projected_outflow,
    net_cashflow,
    -- Cumulative cashflow over the forecast window
    sum(net_cashflow) over (
        partition by portfolio_id, partition_date
        order by forecast_date
        rows between unbounded preceding and current row
    ) as cumulative_cashflow,
    partition_date,
    current_timestamp() as _computed_at
from forecasted
