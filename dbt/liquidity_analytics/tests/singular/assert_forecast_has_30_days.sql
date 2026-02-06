/*
  Singular test: assert that the cashflow forecast has exactly
  30 rows per portfolio (one per forecast day).
  Catches issues with the date sequence generation.
*/

select
    portfolio_id,
    partition_date,
    count(*) as forecast_days
from {{ ref('mart_cashflow_forecast') }}
where partition_date = '{{ var("partition_date") }}'
group by portfolio_id, partition_date
having count(*) != 30
