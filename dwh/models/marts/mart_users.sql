{{
    config(
        materialized='table',
    )
}}

-- Consumer-facing users table built on stg_users.
select
    user_id,
    email,
    concat(name, ' ', surname) as full_name,
    registration_date,
    date_diff(current_date(), registration_date, day) as account_age_days
from {{ ref('stg_users') }}
