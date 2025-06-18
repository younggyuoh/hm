{{ config(
    materialized='table'
) }}

-- In a production environment, this table would be updated incrementally on a daily basis.
-- For this small-scale assignment, partitioning was omitted for simplicity.

with user_cohort as (
  select user_id,
         date_trunc('week', cast(first_event_date as date)) as cohort_week
    from {{ ref('s_user_total_activity_d') }}
),

user_activity as (
  select user_id,
         cast(week_start_date as date) as activity_week
    from {{ ref('s_user_activity_w') }}
),

cohort_activity as (
  select uc.cohort_week,
         ua.activity_week,
         datediff('week', uc.cohort_week, ua.activity_week) as week_offset,
         uc.user_id
    from user_cohort uc
         join user_activity ua on uc.user_id = ua.user_id
   where ua.activity_week >= uc.cohort_week
),

retention_counts as (
  select cohort_week,
         week_offset,
         count(distinct user_id) as retained_users
    from cohort_activity
   group by cohort_week, week_offset
),

cohort_sizes as (
  select cohort_week,
         count(distinct user_id) as cohort_size
    from user_cohort
   group by cohort_week
)

select r.cohort_week,
       r.week_offset,
       r.retained_users,
       c.cohort_size,
       round(cast(r.retained_users as double) / nullif(c.cohort_size, 0), 4) as retention_rate,
       strftime(current_timestamp, '%Y-%m-%d %H:%M:%S.%f') as etl_at_utc,
       replace(strftime(current_date, '%Y-%m-%d'), '-', '') as dt
  from retention_counts r
       join cohort_sizes c on r.cohort_week = c.cohort_week
