{{
    config(
        materialized='incremental',
        incremental_strategy='append',
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
    ingestion_timestamp
from {{ source('raw', 'users') }}
{% if is_incremental() %}
  where FORMAT_TIMESTAMP(ingestion_timestamp, '%Y-%m-%d') > (select max(ingestion_timestamp) from {{ this }})
{% endif %}
