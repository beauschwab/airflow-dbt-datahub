{{
  config(
    materialized='incremental',
    file_format='iceberg',
    unique_key='supplemental_record_id',
    incremental_strategy='merge',
    partition_by=['partition_date'],
    tags=['marts', 'regulatory', 'fr2052a']
  )
}}

/*
  rpt_fr2052a_supplemental.sql
  ----------------------------
  FR 2052a Schedule S: Supplemental Items
  
  Federal Reserve Complex Institution Liquidity Monitoring Report.
  Captures supplemental data including derivatives collateral,
  encumbered assets, and off-balance sheet exposures.
  
  Grain: one row per (portfolio_id, item_type, partition_date)
*/

with positions as (
    select * from {{ ref('mart_positions') }}
    {% if is_incremental() %}
    where partition_date = '{{ var("partition_date") }}'
    {% endif %}
),

liquidity_risk as (
    select * from {{ ref('mart_liquidity_risk') }}
    {% if is_incremental() %}
    where partition_date = '{{ var("partition_date") }}'
    {% endif %}
),

hqla_class as (
    select * from {{ ref('seed_hqla_classification') }}
),

-- Summarize HQLA by portfolio and level
hqla_summary as (
    select
        p.portfolio_id,
        p.partition_date,
        -- Classify by HQLA level (simplified mapping)
        case
            when p.source_system = 'DEPOSITS' then 'NON_HQLA'
            when p.source_system = 'LOANS' then 'NON_HQLA'
            else 'NON_HQLA'
        end as hqla_level,
        sum(p.notional_value) as total_value
    from positions p
    group by p.portfolio_id, p.partition_date,
        case
            when p.source_system = 'DEPOSITS' then 'NON_HQLA'
            when p.source_system = 'LOANS' then 'NON_HQLA'
            else 'NON_HQLA'
        end
),

-- Aggregate liquidity metrics for supplemental reporting
liquidity_metrics as (
    select
        portfolio_id,
        partition_date,
        count(*) as instrument_count,
        sum(market_value) as total_market_value,
        sum(case when liquidity_tier = 'HIGH' then market_value else 0 end) as high_liquidity_value,
        sum(case when liquidity_tier = 'MEDIUM' then market_value else 0 end) as medium_liquidity_value,
        sum(case when liquidity_tier = 'LOW' then market_value else 0 end) as low_liquidity_value,
        avg(est_days_to_liquidate) as avg_days_to_liquidate
    from liquidity_risk
    group by portfolio_id, partition_date
),

-- Combine supplemental items
supplemental_items as (
    -- Item 1: Unencumbered HQLA
    select
        portfolio_id,
        partition_date,
        'S.H.1' as item_code,
        'UNENCUMBERED_HQLA' as item_type,
        'Unencumbered High-Quality Liquid Assets' as item_description,
        total_value as reported_amount,
        0 as haircut_amount,
        total_value as net_amount
    from hqla_summary
    where hqla_level != 'NON_HQLA'
    
    union all
    
    -- Item 2: Total market value by liquidity tier
    select
        portfolio_id,
        partition_date,
        'S.L.1' as item_code,
        'LIQUIDITY_PROFILE' as item_type,
        'Portfolio Liquidity Profile Summary' as item_description,
        total_market_value as reported_amount,
        0 as haircut_amount,
        high_liquidity_value as net_amount  -- Report high liquidity as net
    from liquidity_metrics
    
    union all
    
    -- Item 3: Encumbered assets (positions with HEDGE type assumed encumbered)
    select
        portfolio_id,
        partition_date,
        'S.E.1' as item_code,
        'ENCUMBERED_ASSETS' as item_type,
        'Assets Pledged as Collateral' as item_description,
        sum(notional_value) as reported_amount,
        sum(notional_value) as haircut_amount,  -- Fully haircut (not available)
        0 as net_amount
    from positions
    where position_type = 'HEDGE'
    group by portfolio_id, partition_date
    
    union all
    
    -- Item 4: Off-balance sheet exposures (SHORT positions as contingent)
    select
        portfolio_id,
        partition_date,
        'S.O.1' as item_code,
        'OFF_BALANCE_SHEET' as item_type,
        'Contingent Obligations and Commitments' as item_description,
        sum(abs(notional_value)) as reported_amount,
        sum(abs(notional_value)) * 0.05 as haircut_amount,  -- 5% draw assumption
        sum(abs(notional_value)) * 0.05 as net_amount
    from positions
    where position_type = 'SHORT'
    group by portfolio_id, partition_date
)

select
    {{ dbt_utils.generate_surrogate_key([
        'portfolio_id', 'item_code', 'partition_date'
    ]) }} as supplemental_record_id,
    portfolio_id,
    partition_date,
    'S' as schedule,
    item_code,
    item_type,
    item_description,
    round(reported_amount, 2) as reported_amount,
    round(haircut_amount, 2) as haircut_amount,
    round(net_amount, 2) as net_amount,
    current_timestamp() as _computed_at
from supplemental_items
where reported_amount is not null 
  and reported_amount != 0
