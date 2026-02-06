{{
  config(
    materialized='incremental',
    file_format='iceberg',
    unique_key='position_id',
    incremental_strategy='merge',
    partition_by=['partition_date'],
    tags=['staging']
  )
}}

/*
  stg_positions_loans.sql
  -----------------------
  Staging model: dedup, type-cast, and partition raw loan positions
  from the Loans Management System (LMS).

  Grain: one row per (portfolio_id, instrument_id, position_type, partition_date)
*/

with source as (
    select
        *,
        row_number() over (
            partition by portfolio_id, instrument_id, position_type, partition_date
            order by _ingestion_ts desc
        ) as _row_num
    from {{ source('raw', 'positions_loans') }}
    {% if is_incremental() %}
    where partition_date = '{{ var("partition_date") }}'
    {% endif %}
),

deduped as (
    select * from source where _row_num = 1
)

select
    {{ dbt_utils.generate_surrogate_key([
        'portfolio_id', 'instrument_id', 'position_type', 'partition_date'
    ]) }} as position_id,
    cast(portfolio_id as string)            as portfolio_id,
    cast(instrument_id as string)           as instrument_id,
    cast(position_qty as decimal(18,4))     as position_qty,
    cast(notional_value as decimal(18,2))   as notional_value,
    cast(position_type as string)           as position_type,
    cast(currency as string)                as currency,
    cast(maturity_date as date)             as maturity_date,
    cast(interest_rate as decimal(10,6))    as interest_rate,
    cast('{{ var("partition_date") }}' as date) as partition_date,
    current_timestamp()                     as _loaded_at
from deduped
