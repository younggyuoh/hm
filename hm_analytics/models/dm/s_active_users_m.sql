{{ config(
    materialized='table'
) }}

-- In a production environment, this table would be updated incrementally on a daily basis.
-- For this small-scale assignment, partitioning was omitted for simplicity.

select d.month_start_date,
       d.month,
       coalesce(e.transaction_category, 'all') as transaction_category,
       coalesce(e.country, 'all')  as country,
       count(distinct e.user_id) as mau,
       replace(d.month_start_date, '-', '') as dt
  from {{ ref('fct_events_d') }} e
       join {{ source('manual_loaded','dim_date') }} d
       on e.event_date_utc = d.base_date
 group by d.month_start_date, d.month,
 grouping sets (
    (e.country),
    (e.transaction_category),
    (e.country, e.transaction_category),
    ()
  )
