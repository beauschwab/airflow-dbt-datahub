{{
  config(
    materialized='incremental',
    file_format='iceberg',
    unique_key='nsfr_record_id',
    incremental_strategy='merge',
    partition_by=['partition_date'],
    tags=['marts', 'regulatory']
  )
}}

/*
  rpt_nsfr.sql
  ------------
  Net Stable Funding Ratio (NSFR) per Basel III standards.
  
  Formula: NSFR = Available Stable Funding (ASF) / Required Stable Funding (RSF) >= 100%
  
  ASF = Sum of liabilities and capital weighted by stability factor
  RSF = Sum of assets weighted by liquidity factor
  
  Grain: one row per (portfolio_id, partition_date)
*/

with positions as (
    select * from {{ ref('mart_positions') }}
    {% if is_incremental() %}
    where partition_date = '{{ var("partition_date") }}'
    {% endif %}
),

nsfr_factors as (
    select * from {{ ref('seed_nsfr_factors') }}
),

-- Classify positions by instrument type and maturity bucket
position_classified as (
    select
        p.portfolio_id,
        p.instrument_id,
        p.notional_value,
        p.source_system,
        p.maturity_date,
        p.partition_date,
        -- Determine instrument type for NSFR purposes
        case
            when p.source_system = 'DEPOSITS' and p.position_type = 'LONG' then 'RETAIL_DEPOSIT_STABLE'
            when p.source_system = 'DEPOSITS' then 'RETAIL_DEPOSIT_LESS_STABLE'
            when p.source_system = 'LOANS' then 'LOAN_RETAIL'
            else 'OTHER_ASSET'
        end as instrument_type,
        -- Determine maturity bucket
        case
            when p.maturity_date is null then 'ANY'
            when datediff(p.maturity_date, p.partition_date) < 180 then 'LT_6M'
            when datediff(p.maturity_date, p.partition_date) < 365 then 'GTE_6M_LT_1Y'
            else 'GTE_1Y'
        end as maturity_bucket
    from positions p
),

-- Join with NSFR factors to get ASF and RSF weights
position_weighted as (
    select
        pc.portfolio_id,
        pc.instrument_id,
        pc.notional_value,
        pc.source_system,
        pc.instrument_type,
        pc.maturity_bucket,
        pc.partition_date,
        coalesce(nf.asf_factor_pct, 0) as asf_factor_pct,
        coalesce(nf.rsf_factor_pct, 50) as rsf_factor_pct,  -- Default 50% for unclassified
        -- Calculate ASF contribution (liabilities/capital side)
        case
            when pc.source_system = 'DEPOSITS'  -- Deposits are funding sources
            then pc.notional_value * coalesce(nf.asf_factor_pct, 0) / 100
            else 0
        end as asf_contribution,
        -- Calculate RSF contribution (asset side)
        case
            when pc.source_system = 'LOANS'  -- Loans require stable funding
            then pc.notional_value * coalesce(nf.rsf_factor_pct, 50) / 100
            else 0
        end as rsf_contribution
    from position_classified pc
    left join nsfr_factors nf
        on pc.instrument_type = nf.instrument_type
        and (pc.maturity_bucket = nf.maturity_bucket or nf.maturity_bucket = 'ANY')
),

-- Aggregate by portfolio
nsfr_by_portfolio as (
    select
        portfolio_id,
        partition_date,
        -- ASF components by category
        sum(case 
            when instrument_type like 'RETAIL_DEPOSIT%' then asf_contribution 
            else 0 
        end) as asf_retail_deposits,
        sum(case 
            when instrument_type like 'WHOLESALE%' then asf_contribution 
            else 0 
        end) as asf_wholesale_funding,
        sum(asf_contribution) as total_asf,
        -- RSF components by category
        sum(case 
            when instrument_type like 'LOAN%' then rsf_contribution 
            else 0 
        end) as rsf_loans,
        sum(case 
            when instrument_type like 'HQLA%' then rsf_contribution 
            else 0 
        end) as rsf_hqla,
        sum(case 
            when instrument_type like 'OTHER%' then rsf_contribution 
            else 0 
        end) as rsf_other_assets,
        sum(rsf_contribution) as total_rsf
    from position_weighted
    group by portfolio_id, partition_date
)

select
    {{ dbt_utils.generate_surrogate_key([
        'portfolio_id', 'partition_date'
    ]) }} as nsfr_record_id,
    portfolio_id,
    partition_date,
    -- ASF breakdown
    round(asf_retail_deposits, 2) as asf_retail_deposits,
    round(asf_wholesale_funding, 2) as asf_wholesale_funding,
    round(total_asf, 2) as total_asf,
    -- RSF breakdown
    round(rsf_loans, 2) as rsf_loans,
    round(rsf_hqla, 2) as rsf_hqla,
    round(rsf_other_assets, 2) as rsf_other_assets,
    round(total_rsf, 2) as total_rsf,
    -- NSFR calculation
    case
        when total_rsf > 0
        then round(total_asf / total_rsf * 100, 2)
        else 999.99  -- No required funding = effectively infinite NSFR
    end as nsfr_pct,
    -- Compliance flag (>= 100% required)
    case
        when total_rsf <= 0 then true
        when total_asf / total_rsf >= 1.0 then true
        else false
    end as is_nsfr_compliant,
    -- Surplus/deficit
    round(total_asf - total_rsf, 2) as nsfr_surplus_deficit,
    current_timestamp() as _computed_at
from nsfr_by_portfolio
