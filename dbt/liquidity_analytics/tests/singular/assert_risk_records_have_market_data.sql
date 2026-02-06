/*
  Singular test: assert that every position in the mart has
  a corresponding market data record. Catches cases where
  the join dropped records unexpectedly.
*/

select
    r.risk_record_id,
    r.instrument_id,
    r.partition_date
from {{ ref('mart_liquidity_risk') }} r
left join {{ ref('stg_market_data') }} m
    on r.instrument_id = m.instrument_id
    and r.partition_date = m.partition_date
where m.market_data_id is null
  and r.partition_date = '{{ var("partition_date") }}'
