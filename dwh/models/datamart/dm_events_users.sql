{{
    config(
        materialized='table',
    )
}}

-- Event-level data (no aggregation) enriched with user attributes.
select
    e.event_id,
    e.user_id,
    e.event_type,
    e.event_attributes,
    e.ingestion_timestamp,
    e.ingestion_date,
    u.email,
    u.name,
    u.surname,
    u.registration_date,
    u.ip,
    u.passport_number
from {{ ref('silver_events') }} as e
left join {{ ref('gold_users') }} as u
    on e.user_id = u.user_id
