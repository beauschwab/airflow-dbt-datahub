{{
  config(
    materialized='incremental',
    file_format='iceberg',
    unique_key='market_data_id',
    incremental_strategy='merge',
    partition_by=['partition_date'],
    tags=['staging']
  )
}}

/*
  stg_market_data.sql
  -------------------
  Staging model: dedup, type-cast, and partition raw market data.
  Reads from Iceberg staging table populated by Spark ingestion.

  Grain: one row per (instrument_id, as_of_date)
*/

with source as (
    select
        *,
        row_number() over (
            partition by instrument_id, as_of_date
            order by _ingestion_ts desc
        ) as _row_num
    from {{ source('raw', 'market_data') }}
    {% if is_incremental() %}
    where partition_date = '{{ var("partition_date") }}'
    {% endif %}
),

deduped as (
    select * from source where _row_num = 1
)

select
    {{ dbt_utils.generate_surrogate_key(['instrument_id', 'as_of_date']) }}
        as market_data_id,
    cast(instrument_id as string)       as instrument_id,
    cast(as_of_date as date)            as as_of_date,
    cast(close_price as decimal(18,6))  as close_price,
    cast(bid_price as decimal(18,6))    as bid_price,
    cast(ask_price as decimal(18,6))    as ask_price,
    cast(volume as bigint)              as volume,
    cast(yield_rate as decimal(10,6))   as yield_rate,
    cast(spread_bps as decimal(10,2))   as spread_bps,
    cast('{{ var("partition_date") }}' as date) as partition_date,
    current_timestamp()                 as _loaded_at
from deduped
