{# Rejected records per contract and day, so data quality has a trend line. #}

select
    cast(contract as varchar) as contract,
    cast(cast(_ingested_at as date) as date) as ingested_date,
    cast(reason as varchar) as reason,
    cast(count(*) as bigint) as rejected_records
from {{ source('bronze', 'quarantine') }}
group by contract, cast(_ingested_at as date), reason
