{{
  config(
    materialized='incremental',
    file_format='iceberg',
    unique_key='lcr_record_id',
    incremental_strategy='merge',
    partition_by=['partition_date'],
    tags=['marts', 'regulatory']
  )
}}

/*
  rpt_lcr.sql
  -----------
  Liquidity Coverage Ratio (LCR) per Basel III standards.
  
  Formula: LCR = HQLA / Total Net Cash Outflows (30-day horizon) >= 100%
  
  HQLA is calculated with Level 1, 2A, 2B haircuts applied.
  Net outflows = Total outflows - min(Inflows, 75% of outflows)
  
  Grain: one row per (portfolio_id, partition_date)
*/

with positions as (
    select * from {{ ref('mart_positions') }}
    {% if is_incremental() %}
    where partition_date = '{{ var("partition_date") }}'
    {% endif %}
),

cashflows as (
    select * from {{ ref('mart_cashflow_forecast') }}
    {% if is_incremental() %}
    where partition_date = '{{ var("partition_date") }}'
    {% endif %}
),

hqla_class as (
    select * from {{ ref('seed_hqla_classification') }}
),

-- Classify positions into HQLA levels and apply haircuts
position_hqla as (
    select
        p.portfolio_id,
        p.instrument_id,
        p.notional_value,
        p.source_system,
        p.partition_date,
        -- Map position type to HQLA classification
        -- In production, this would join on instrument master data
        case
            when p.source_system = 'DEPOSITS' then 'DEMAND_DEPOSIT'
            when p.source_system = 'LOANS' and p.maturity_date is null then 'LOAN'
            when p.source_system = 'LOANS' then 'LOAN'
            else 'OTHER'
        end as instrument_type,
        coalesce(h.hqla_level, 'NON_HQLA') as hqla_level,
        coalesce(h.haircut_pct, 100) as haircut_pct
    from positions p
    left join hqla_class h
        on case
            when p.source_system = 'DEPOSITS' then 'DEMAND_DEPOSIT'
            when p.source_system = 'LOANS' then 'LOAN'
            else 'OTHER'
        end = h.instrument_type
),

-- Calculate HQLA by level with haircuts
hqla_by_portfolio as (
    select
        portfolio_id,
        partition_date,
        -- Level 1 HQLA (no haircut cap)
        sum(case 
            when hqla_level = '1' 
            then notional_value * (1 - haircut_pct / 100)
            else 0 
        end) as hqla_level_1,
        -- Level 2A HQLA (max 40% of total HQLA)
        sum(case 
            when hqla_level = '2A' 
            then notional_value * (1 - haircut_pct / 100)
            else 0 
        end) as hqla_level_2a_raw,
        -- Level 2B HQLA (max 15% of total HQLA, included in 2A cap)
        sum(case 
            when hqla_level = '2B' 
            then notional_value * (1 - haircut_pct / 100)
            else 0 
        end) as hqla_level_2b_raw
    from position_hqla
    where notional_value > 0  -- Assets only
    group by portfolio_id, partition_date
),

-- Aggregate 30-day cashflow outflows and inflows
cashflow_30d as (
    select
        portfolio_id,
        partition_date,
        sum(case when projected_outflow > 0 then projected_outflow else 0 end) as total_outflows_30d,
        sum(case when projected_inflow > 0 then projected_inflow else 0 end) as total_inflows_30d
    from cashflows
    where forecast_date <= date_add(partition_date, 30)
    group by portfolio_id, partition_date
),

-- Calculate LCR components
lcr_calc as (
    select
        h.portfolio_id,
        h.partition_date,
        h.hqla_level_1,
        h.hqla_level_2a_raw,
        h.hqla_level_2b_raw,
        -- Apply Level 2 caps per Basel III
        -- Level 2B capped at 15% of total HQLA
        -- Level 2A + 2B capped at 40% of total HQLA
        h.hqla_level_1 + 
            least(
                h.hqla_level_2a_raw + h.hqla_level_2b_raw,
                h.hqla_level_1 * 0.4 / 0.6  -- 40% cap means L2 <= L1 * 40/60
            ) as total_hqla,
        coalesce(c.total_outflows_30d, 0) as total_outflows_30d,
        coalesce(c.total_inflows_30d, 0) as total_inflows_30d,
        -- Inflows capped at 75% of outflows per Basel III
        least(
            coalesce(c.total_inflows_30d, 0),
            coalesce(c.total_outflows_30d, 0) * 0.75
        ) as capped_inflows_30d
    from hqla_by_portfolio h
    left join cashflow_30d c
        on h.portfolio_id = c.portfolio_id
        and h.partition_date = c.partition_date
)

select
    {{ dbt_utils.generate_surrogate_key([
        'portfolio_id', 'partition_date'
    ]) }} as lcr_record_id,
    portfolio_id,
    partition_date,
    -- HQLA breakdown
    round(hqla_level_1, 2) as hqla_level_1,
    round(hqla_level_2a_raw, 2) as hqla_level_2a,
    round(hqla_level_2b_raw, 2) as hqla_level_2b,
    round(total_hqla, 2) as total_hqla,
    -- Cash flow components
    round(total_outflows_30d, 2) as total_outflows_30d,
    round(total_inflows_30d, 2) as total_inflows_30d,
    round(capped_inflows_30d, 2) as capped_inflows_30d,
    -- Net cash outflows
    round(total_outflows_30d - capped_inflows_30d, 2) as net_cash_outflows_30d,
    -- LCR calculation
    case
        when (total_outflows_30d - capped_inflows_30d) > 0
        then round(total_hqla / (total_outflows_30d - capped_inflows_30d) * 100, 2)
        else 999.99  -- No net outflows = effectively infinite LCR
    end as lcr_pct,
    -- Compliance flag (>= 100% required)
    case
        when (total_outflows_30d - capped_inflows_30d) <= 0 then true
        when total_hqla / (total_outflows_30d - capped_inflows_30d) >= 1.0 then true
        else false
    end as is_lcr_compliant,
    current_timestamp() as _computed_at
from lcr_calc
