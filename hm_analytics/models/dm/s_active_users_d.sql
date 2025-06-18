{{ config(
    materialized='table'
) }}

-- In a production environment, this table would be updated incrementally on a daily basis.
-- For this small-scale assignment, partitioning was omitted for simplicity.

select e.event_date_utc,
       coalesce(e.transaction_category, 'all') as transaction_category,
       coalesce(e.country, 'all')  as country,
       count(distinct e.user_id) as dau,
       replace(event_date_utc, '-', '') as dt
  from {{ ref('fct_events_d') }} e
 group by e.event_date_utc,
 grouping sets (
    (e.country),
    (e.transaction_category),
    (e.country, e.transaction_category),
    ()
  )
