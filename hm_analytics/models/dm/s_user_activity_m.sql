{{ config(
    materialized='table'
) }}
-- In a production environment, this table would be updated incrementally on a daily basis.
-- For this small-scale assignment, partitioning was omitted for simplicity.

select d.month_start_date,
       d.month,
       user_id,
       count(1) total_event_count,
       sum(miles_amount) total_miles_amount,
       strftime(current_timestamp, '%Y-%m-%d %H:%M:%S.%f')  as etl_at_utc,
       replace(d.month_start_date, '-', '') as dt
  from {{ ref('fct_events_d') }} f
       inner join {{ source('manual_loaded','dim_date') }} d
       on f.event_date_utc = d.base_date
 group by d.month_start_date,d.month,user_id
