{{
    config(
        materialized='incremental',
    )
}}

select
    event_id,
    user_id,
    event_type,
    event_attributes,
    ingestion_timestamp,
    date(ingestion_timestamp) as ingestion_date,
    current_timestamp() as dwh_created_dttm,
from {{ source('raw', 'events') }}
where ingestion_timestamp >= timestamp_sub(
      (select max(ingestion_timestamp) from {{ this }}),
      interval {{ var('silver_lookback_days') }} day
  )
