/*
  Singular test: assert that mart_positions contains records from
  BOTH source systems (LOANS and DEPOSITS) for the current partition.
  If either system fails to contribute positions, this test flags it.
*/

with source_counts as (
    select
        source_system,
        count(*) as row_count
    from {{ ref('mart_positions') }}
    where partition_date = '{{ var("partition_date") }}'
    group by source_system
),

expected_systems as (
    select 'LOANS' as expected_source
    union all
    select 'DEPOSITS' as expected_source
)

-- Return rows for any missing source system → test fails if any row returned
select
    e.expected_source as missing_source_system
from expected_systems e
left join source_counts s
    on e.expected_source = s.source_system
where s.source_system is null
