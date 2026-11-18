{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='user_id',
        merge_exclude_columns=['dwh_created_dttm'],
        partition_by={'field': 'dwh_modified_dttm', 'data_type': 'timestamp', 'granularity': 'day'},
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
    ingestion_timestamp,
    current_timestamp() as dwh_created_dttm,
    current_timestamp() as dwh_modified_dttm
from {{ ref('silver_users') }}
{% if is_incremental() %}
where ingestion_timestamp > (select max(ingestion_timestamp) from {{ this }})
{% endif %}
