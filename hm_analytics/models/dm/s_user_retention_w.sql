{{ config(
    materialized='table'
) }}

-- In a production environment, this table would be updated incrementally on a daily basis.
-- For this small-scale assignment, partitioning was omitted for simplicity.

with current_week_users as (
  select ua.user_id,
         ua.week_start_date as activity_week,
         strftime(cast(ua.week_start_date as date) - interval 1 week, '%Y-%m-%d') as prev_week,
         strftime(date_trunc('week', cast(uta.first_event_date as date)),'%Y-%m-%d') as user_first_week
    from {{ ref('s_user_activity_w') }} ua
         left join {{ ref('s_user_total_activity_d') }} uta on ua.user_id = uta.user_id
),

prev_week_users as (
  select user_id,
         week_start_date as activity_week
    from {{ ref('s_user_activity_w') }}
),

retention as (
  select cmu.activity_week as week_start,
         cmu.user_id,
         case
           when cmu.user_first_week = cmu.activity_week then 'new'
           when pmu.user_id is not null then 'retained'
           else 'resurrected'
         end as user_status
    from current_week_users cmu
         left join prev_week_users pmu
         on cmu.user_id = pmu.user_id
         and pmu.activity_week = cmu.prev_week
),
churn as (
  select pmu.activity_week,
         strftime(cast(pmu.activity_week as date) + interval 1 week, '%Y-%m-%d') as week_start,
         pmu.user_id,
         'churned' as user_status
    from prev_week_users pmu
         left join current_week_users cmu
         on pmu.user_id = cmu.user_id
         and cast(cmu.activity_week as date) = cast(pmu.activity_week as date) + interval 1 week
   where cmu.user_id is null
     and pmu.activity_week < '2025-06-02'
)
select week_start,
       sum(case when user_status = 'new' then 1 else 0 end) as new_user_count,
       sum(case when user_status = 'retained' then 1 else 0 end) as retained_user_count,
       sum(case when user_status = 'resurrected' then 1 else 0 end) as resurrected_user_count,
       sum(case when user_status = 'churned' then 1 else 0 end) as churned_user_count,
       strftime(current_timestamp, '%Y-%m-%d %H:%M:%S.%f') as etl_at_utc,
       replace(week_start, '-', '') as dt
from (select week_start,
             user_id,
             user_status
        from retention
      union all
      select week_start,
             user_id,
             user_status
        from churn
     )
group by week_start


