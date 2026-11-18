{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
    )
}}

-- Aggregate: events per user, per event type, per day.
select
    ingestion_date,
    user_id,
    event_type,
    count(*) as events_count,
    min(ingestion_timestamp) as first_ingested_at,
    max(ingestion_timestamp) as last_ingested_at
from {{ ref('silver_events') }}
{% if is_incremental() %}
where ingestion_date >= date_sub(current_date(), interval {{ var('silver_lookback_days') }} day)
{% endif %}
group by 1, 2, 3
