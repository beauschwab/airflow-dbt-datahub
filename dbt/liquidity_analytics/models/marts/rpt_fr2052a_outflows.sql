{{
  config(
    materialized='incremental',
    file_format='iceberg',
    unique_key='outflow_record_id',
    incremental_strategy='merge',
    partition_by=['partition_date'],
    tags=['marts', 'regulatory', 'fr2052a']
  )
}}

/*
  rpt_fr2052a_outflows.sql
  ------------------------
  FR 2052a Schedule O: Outflow Amounts
  
  Federal Reserve Complex Institution Liquidity Monitoring Report.
  Maps positions to regulatory product categories and calculates
  stressed cash outflows by maturity bucket.
  
  Grain: one row per (portfolio_id, product_code, maturity_bucket, partition_date)
*/

with positions as (
    select * from {{ ref('mart_positions') }}
    {% if is_incremental() %}
    where partition_date = '{{ var("partition_date") }}'
    {% endif %}
),

product_mapping as (
    select * from {{ ref('seed_fr2052a_product_mapping') }}
    where schedule = 'O'  -- Outflows only
),

-- Classify positions into FR 2052a product categories
position_classified as (
    select
        p.portfolio_id,
        p.instrument_id,
        p.notional_value,
        p.source_system,
        p.maturity_date,
        p.partition_date,
        -- Map to FR 2052a product category based on position characteristics
        case
            -- Retail deposits
            when p.source_system = 'DEPOSITS' and p.position_type = 'LONG' 
                and p.notional_value < 250000 then 'RETAIL_STABLE'
            when p.source_system = 'DEPOSITS' and p.position_type = 'LONG' then 'RETAIL_LESS_STABLE'
            -- Wholesale funding
            when p.source_system = 'DEPOSITS' and p.position_type != 'LONG' then 'WHOLESALE_NON_OP_UNSECURED'
            -- Credit facilities (loans as potential outflows via undrawn commitments)
            when p.source_system = 'LOANS' and p.position_type = 'HEDGE' then 'CREDIT_FACILITY_CORP'
            else null  -- Not an outflow item
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
    where p.source_system = 'DEPOSITS'  -- Deposits represent potential outflows
       or (p.source_system = 'LOANS' and p.position_type = 'HEDGE')  -- Undrawn commitments
),

-- Join with product mapping for run-off rates
position_with_runoff as (
    select
        pc.portfolio_id,
        pc.instrument_id,
        pc.notional_value,
        pc.product_category,
        pc.maturity_bucket,
        pc.partition_date,
        pm.product_code,
        pm.sub_schedule,
        coalesce(pm.run_off_rate_pct, 100) as run_off_rate_pct,
        pm.description as product_description,
        -- Calculate stressed outflow amount
        pc.notional_value * coalesce(pm.run_off_rate_pct, 100) / 100 as stressed_outflow
    from position_classified pc
    left join product_mapping pm
        on pc.product_category = pm.product_category
        and (pc.maturity_bucket = pm.maturity_bucket 
             or pm.maturity_bucket = 'OPEN' 
             or pm.maturity_bucket = 'ANY')
    where pc.product_category is not null
),

-- Aggregate by product code and maturity bucket
outflows_aggregated as (
    select
        portfolio_id,
        partition_date,
        product_code,
        sub_schedule,
        maturity_bucket,
        product_description,
        run_off_rate_pct,
        count(*) as position_count,
        sum(notional_value) as gross_amount,
        sum(stressed_outflow) as stressed_outflow_amount
    from position_with_runoff
    group by 
        portfolio_id,
        partition_date,
        product_code,
        sub_schedule,
        maturity_bucket,
        product_description,
        run_off_rate_pct
)

select
    {{ dbt_utils.generate_surrogate_key([
        'portfolio_id', 'product_code', 'maturity_bucket', 'partition_date'
    ]) }} as outflow_record_id,
    portfolio_id,
    partition_date,
    'O' as schedule,
    sub_schedule,
    product_code,
    product_description,
    maturity_bucket,
    run_off_rate_pct,
    position_count,
    round(gross_amount, 2) as gross_amount,
    round(stressed_outflow_amount, 2) as stressed_outflow_amount,
    -- Forward maturity buckets for filing
    case maturity_bucket
        when 'OPEN' then 1
        when 'LT_30D' then 2
        when 'GTE_30D_LT_60D' then 3
        when 'GTE_60D_LT_90D' then 4
        when 'GTE_90D' then 5
    end as maturity_bucket_order,
    current_timestamp() as _computed_at
from outflows_aggregated
