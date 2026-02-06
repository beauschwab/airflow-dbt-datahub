{{
  config(
    materialized='incremental',
    file_format='iceberg',
    unique_key='position_id',
    incremental_strategy='merge',
    partition_by=['partition_date', 'source_system'],
    tags=['marts']
  )
}}

/*
  mart_positions.sql
  ------------------
  Mart model: unified position book combining loans and deposits.
  Each source system lands as a separate partition value in the
  `source_system` column, enabling efficient partition pruning by
  downstream consumers who only care about one product type.

  Partitioning:
    - partition_date   : business date (daily)
    - source_system    : 'LOANS' | 'DEPOSITS' (product type)

  Grain: one row per (portfolio_id, instrument_id, position_type,
         source_system, partition_date)
*/

with loans as (
    select
        position_id,
        portfolio_id,
        instrument_id,
        position_qty,
        notional_value,
        position_type,
        currency,
        maturity_date,
        interest_rate,
        partition_date,
        _loaded_at,
        'LOANS' as source_system
    from {{ ref('stg_positions_loans') }}
    {% if is_incremental() %}
    where partition_date = '{{ var("partition_date") }}'
    {% endif %}
),

deposits as (
    select
        position_id,
        portfolio_id,
        instrument_id,
        position_qty,
        notional_value,
        position_type,
        currency,
        maturity_date,
        interest_rate,
        partition_date,
        _loaded_at,
        'DEPOSITS' as source_system
    from {{ ref('stg_positions_deposits') }}
    {% if is_incremental() %}
    where partition_date = '{{ var("partition_date") }}'
    {% endif %}
),

unioned as (
    select * from loans
    union all
    select * from deposits
)

select
    -- Re-key to include source_system in the surrogate key
    {{ dbt_utils.generate_surrogate_key([
        'portfolio_id', 'instrument_id', 'position_type',
        'source_system', 'partition_date'
    ]) }} as position_id,
    portfolio_id,
    instrument_id,
    position_qty,
    notional_value,
    position_type,
    currency,
    maturity_date,
    interest_rate,
    source_system,
    partition_date,
    _loaded_at,
    current_timestamp() as _unified_at
from unioned
