{{
  config(
    materialized='incremental',
    file_format='iceberg',
    unique_key='inflow_record_id',
    incremental_strategy='merge',
    partition_by=['partition_date'],
    tags=['marts', 'regulatory', 'fr2052a']
  )
}}

/*
  rpt_fr2052a_inflows.sql
  -----------------------
  FR 2052a Schedule I: Inflow Amounts
  
  Federal Reserve Complex Institution Liquidity Monitoring Report.
  Maps loan positions and receivables to regulatory product categories
  and calculates expected cash inflows by maturity bucket.
  
  Grain: one row per (portfolio_id, product_code, maturity_bucket, partition_date)
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

product_mapping as (
    select * from {{ ref('seed_fr2052a_product_mapping') }}
    where schedule = 'I'  -- Inflows only
),

-- Classify loan positions into FR 2052a inflow categories
position_classified as (
    select
        p.portfolio_id,
        p.instrument_id,
        p.notional_value,
        p.source_system,
        p.maturity_date,
        p.interest_rate,
        p.partition_date,
        -- Map to FR 2052a product category
        case
            -- Retail loan repayments
            when p.source_system = 'LOANS' and p.position_type = 'LONG' 
                and p.notional_value < 1000000 then 'INFLOW_RETAIL_LOAN'
            -- Wholesale/corporate loan repayments
            when p.source_system = 'LOANS' and p.position_type = 'LONG' then 'INFLOW_WHOLESALE'
            else null  -- Not an inflow item
        end as product_category,
        -- Maturity bucket for FR 2052a
        case
            when p.maturity_date is null then 'OPEN'
            when datediff(p.maturity_date, p.partition_date) < 30 then 'LT_30D'
            when datediff(p.maturity_date, p.partition_date) < 60 then 'GTE_30D_LT_60D'
            when datediff(p.maturity_date, p.partition_date) < 90 then 'GTE_60D_LT_90D'
            else 'GTE_90D'
        end as maturity_bucket
    from positions p
    where p.source_system = 'LOANS'  -- Loans represent potential inflows
),

-- Join with product mapping for inflow rates
position_with_inflow_rate as (
    select
        pc.portfolio_id,
        pc.instrument_id,
        pc.notional_value,
        pc.interest_rate,
        pc.product_category,
        pc.maturity_bucket,
        pc.partition_date,
        pm.product_code,
        pm.sub_schedule,
        coalesce(pm.run_off_rate_pct, 50) as inflow_rate_pct,  -- Default 50% haircut
        pm.description as product_description,
        -- Calculate expected inflow (principal + accrued interest for 30D)
        (pc.notional_value + pc.notional_value * coalesce(pc.interest_rate, 0) * 30 / 365) 
            * coalesce(pm.run_off_rate_pct, 50) / 100 as expected_inflow
    from position_classified pc
    left join product_mapping pm
        on pc.product_category = pm.product_category
        and (pc.maturity_bucket = pm.maturity_bucket 
             or pm.maturity_bucket = 'ANY')
    where pc.product_category is not null
),

-- Aggregate by product code and maturity bucket
inflows_aggregated as (
    select
        portfolio_id,
        partition_date,
        product_code,
        sub_schedule,
        maturity_bucket,
        product_description,
        inflow_rate_pct,
        count(*) as position_count,
        sum(notional_value) as gross_amount,
        sum(expected_inflow) as expected_inflow_amount
    from position_with_inflow_rate
    group by 
        portfolio_id,
        partition_date,
        product_code,
        sub_schedule,
        maturity_bucket,
        product_description,
        inflow_rate_pct
)

select
    {{ dbt_utils.generate_surrogate_key([
        'portfolio_id', 'product_code', 'maturity_bucket', 'partition_date'
    ]) }} as inflow_record_id,
    portfolio_id,
    partition_date,
    'I' as schedule,
    sub_schedule,
    product_code,
    product_description,
    maturity_bucket,
    inflow_rate_pct,
    position_count,
    round(gross_amount, 2) as gross_amount,
    round(expected_inflow_amount, 2) as expected_inflow_amount,
    -- Forward maturity buckets for filing
    case maturity_bucket
        when 'OPEN' then 1
        when 'LT_30D' then 2
        when 'GTE_30D_LT_60D' then 3
        when 'GTE_60D_LT_90D' then 4
        when 'GTE_90D' then 5
    end as maturity_bucket_order,
    current_timestamp() as _computed_at
from inflows_aggregated
