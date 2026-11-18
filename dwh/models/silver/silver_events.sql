{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
    )
}}

select
    event_id,
    user_id,
    event_type,
    safe.parse_json(event_attributes) as event_attributes,
    ingestion_timestamp,
    date(ingestion_timestamp) as ingestion_date
from {{ source('raw', 'events') }}
where event_id is not null
{% if is_incremental() %}
  and ingestion_timestamp >= timestamp_sub(
      (select max(ingestion_timestamp) from {{ this }}),
      interval {{ var('silver_lookback_days') }} day
  )
{% endif %}
qualify row_number() over (partition by event_id order by ingestion_timestamp desc) = 1
