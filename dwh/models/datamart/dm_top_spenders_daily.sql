{{
    config(
        materialized='table',
    )
}}

-- Returns the right answer: per user, total spend over the last 30 days and the
-- exact number of unique items viewed before their last purchase in the window.
-- It runs daily as a full rebuild.

with recent_events as (
    select *
    from {{ ref('silver_events') }}
    where format_timestamp('%Y-%m-%d', ingestion_timestamp)
        >= format_date('%Y-%m-%d', date_sub(current_date(), interval 29 day))
),

purchases as (
    select
        user_id,
        sum(safe_cast(safe.json_extract_scalar(event_attributes, '$.price') as float64)) as total_spend,
        max(ingestion_timestamp) as last_purchase_at
    from recent_events
    where event_type = 'purchase'
    group by user_id
),

item_views as (
    select
        e.user_id,
        count(distinct safe.json_extract_scalar(e.event_attributes, '$.item_id')) as distinct_items_viewed
    from recent_events as e
    join purchases as p
        on e.user_id = p.user_id
        and e.ingestion_timestamp < p.last_purchase_at
    where e.event_type = 'view_item'
    group by e.user_id
)

select
    p.user_id,
    p.total_spend,
    coalesce(v.distinct_items_viewed, 0) as distinct_items_viewed
from purchases as p
left join item_views as v
    on p.user_id = v.user_id
where p.user_id is not null
order by p.total_spend desc
