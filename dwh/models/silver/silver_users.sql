{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        partition_by={'field': 'dwh_created_dttm', 'data_type': 'timestamp', 'granularity': 'day'},
    )
}}

select
    user_id,
    lower(trim(email)) as email,
    trim(name) as name,
    trim(surname) as surname,
    safe_cast(registration_date as date) as registration_date,
    ip,
    passport_number,
    ingestion_timestamp,
    current_timestamp() as dwh_created_dttm
from {{ source('raw', 'users') }}
{% if is_incremental() %}
  where FORMAT_TIMESTAMP(ingestion_timestamp, '%Y-%m-%d') > (select max(ingestion_timestamp) from {{ this }})
{% endif %}
