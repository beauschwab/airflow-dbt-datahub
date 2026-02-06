{{
  config(
    materialized='incremental',
    file_format='iceberg',
    unique_key='risk_record_id',
    incremental_strategy='merge',
    partition_by=['partition_date'],
    tags=['marts']
  )
}}

/*
  mart_liquidity_risk.sql
  -----------------------
  Mart model: compute daily liquidity risk metrics per portfolio × instrument.
  Joins positions with market data for spread analysis,
  concentration risk, and estimated liquidation timelines.

  Grain: one row per (portfolio_id, instrument_id, partition_date)
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

enriched as (
    select
        p.portfolio_id,
        p.instrument_id,
        p.position_qty,
        p.notional_value,
        m.close_price,
        m.bid_price,
        m.ask_price,
        m.volume,
        m.spread_bps,
        m.yield_rate,
        -- Liquidity classification: higher volume + tighter spread = more liquid
        case
            when m.volume > 1000000 and m.spread_bps < 10 then 'HIGH'
            when m.volume > 100000 and m.spread_bps < 50  then 'MEDIUM'
            else 'LOW'
        end as liquidity_tier,
        -- Estimated days to liquidate at 10% of average daily volume
        case
            when m.volume > 0
            then ceil(abs(p.position_qty) / (m.volume * 0.1))
            else 999
        end as est_days_to_liquidate,
        -- Mark-to-market valuation
        p.position_qty * m.close_price as market_value,
        p.partition_date
    from positions p
    left join market m
        on p.instrument_id = m.instrument_id
        and p.partition_date = m.partition_date
)

select
    {{ dbt_utils.generate_surrogate_key([
        'portfolio_id', 'instrument_id', 'partition_date'
    ]) }} as risk_record_id,
    *,
    current_timestamp() as _computed_at
from enriched
