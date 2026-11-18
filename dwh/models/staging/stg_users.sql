{{
    config(
        materialized='view',
    )
}}

-- One row per user (latest ingested record), cleaned.
-- Sensitive fields (ip, passport_number) are intentionally not exposed.
select
    user_id,
    lower(trim(email)) as email,
    trim(name) as name,
    trim(surname) as surname,
    safe_cast(registration_date as date) as registration_date,
    ingestion_timestamp
from {{ source('raw', 'users') }}
qualify row_number() over (partition by user_id order by ingestion_timestamp desc) = 1
