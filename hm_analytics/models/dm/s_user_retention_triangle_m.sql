{{ config(
    materialized='table'
) }}

-- In a production environment, this table would be updated incrementally on a daily basis.
-- For this small-scale assignment, partitioning was omitted for simplicity.

with user_cohort as (
  select user_id,
         date_trunc('month', cast(first_event_date as date)) as cohort_month
    from {{ ref('s_user_total_activity_d') }}
),

user_activity as (
  select user_id,
         cast(month_start_date as date) as activity_month
    from {{ ref('s_user_activity_m') }}
),

cohort_activity as (
  select uc.cohort_month,
         ua.activity_month,
         datediff('month', uc.cohort_month, ua.activity_month) as month_offset,
         uc.user_id
    from user_cohort uc
         join user_activity ua on uc.user_id = ua.user_id
   where ua.activity_month >= uc.cohort_month
),

retention_counts as (
  select cohort_month,
         month_offset,
         count(distinct user_id) as retained_users
    from cohort_activity
   group by cohort_month, month_offset
),

cohort_sizes as (
  select cohort_month,
         count(distinct user_id) as cohort_size
    from user_cohort
   group by cohort_month
)

select r.cohort_month,
       r.month_offset,
       r.retained_users,
       c.cohort_size,
       round(cast(r.retained_users as double) / nullif(c.cohort_size, 0), 4) as retention_rate,
       strftime(current_timestamp, '%Y-%m-%d %H:%M:%S.%f') as etl_at_utc,
       replace(strftime(current_date, '%Y-%m-%d'), '-', '') as dt
  from retention_counts r
       join cohort_sizes c on r.cohort_month = c.cohort_month
