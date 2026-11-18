{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
    )
}}

select
    user_id,
    email,
    name,
    surname,
    registration_date,
    ip,
    passport_number,
    ingestion_timestamp
from {{ ref('silver_users') }}
{% if is_incremental() %}
where ingestion_timestamp > (select max(ingestion_timestamp) from {{ this }})
{% endif %}
