{{ config(
    materialized='table'
) }}
-- In a production environment, this table would be updated incrementally on a daily basis.
-- For this small-scale assignment, partitioning was omitted for simplicity.

with current_month_users as (
  select ua.user_id,
         ua.month_start_date as activity_month,
         strftime(cast(ua.month_start_date as date) - interval 1 month,'%Y-%m-%d') as prev_month,
         date_trunc('month', cast(uta.first_event_date as date)) as user_first_month
    from {{ ref('s_user_activity_m') }} ua
         left join {{ ref('s_user_total_activity_d') }} uta on ua.user_id = uta.user_id
--    where dt = '20250601'
),
prev_month_users as (
  select user_id,
         month_start_date as activity_month
    from {{ ref('s_user_activity_m') }}
 --    where dt = '20250501'
),
retention as (
  select cmu.activity_month as month_start,
         cmu.user_id,
         case
           when cmu.user_first_month = cmu.activity_month  then 'new'
           when pmu.user_id is not null then 'retained'
           else 'resurrected'
         end as user_status
    from current_month_users cmu
         left join prev_month_users pmu
         on cmu.user_id = pmu.user_id
         and pmu.activity_month = cmu.prev_month
),
churn as (
  select strftime(cast(pmu.activity_month as date) + interval 1 month, '%Y-%m-%d') as month_start,
         pmu.user_id,
         'churned' as user_status
    from prev_month_users pmu
         left join current_month_users cmu
         on pmu.user_id = cmu.user_id
         and cast(cmu.activity_month as date) = cast(pmu.activity_month as date) + interval 1 month
   where cmu.user_id is null
     and pmu.activity_month <= '2025-05-01'
)

select month_start,
       sum(case when user_status = 'new' then 1 else 0 end) as new_user_count,
       sum(case when user_status = 'retained' then 1 else 0 end) as retained_user_count,
       sum(case when user_status = 'resurrected' then 1 else 0 end) as resurrected_user_count,
       sum(case when user_status = 'churned' then 1 else 0 end) as churned_user_count,
       strftime(current_timestamp, '%Y-%m-%d %H:%M:%S.%f') as etl_at_utc,
       replace(month_start, '-', '') as dt
  from (select month_start,
               user_id,
               user_status
          from retention
               union all
        select month_start,
               user_id,
               user_status
          from churn)
 group by month_start
