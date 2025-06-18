{{ config(
    materialized='table'
) }}

-- In a production environment, this table would be updated incrementally on a daily basis.
-- For this small-scale assignment, partitioning was omitted for simplicity.

select user_id, min(event_date_utc) first_event_date,
       max(event_date_utc) last_event_date,
       count(1) total_event_count,
       sum(miles_amount) total_miles_amount,
       STRFTIME(current_timestamp, '%Y-%m-%d %H:%M:%S.%f')  AS etl_at_utc,
       STRFTIME(current_date, '%Y-%m-%d') as dt -- batch_date
  from {{ ref('fct_events_d') }}
 group by user_id

--
--
--
-- with base (
-- select user_id,
-- row_number() over (partition by user_id order by event_time_in_utc) as rn_first, row_number() over (partition by user_id order by event_time_in_utc) as rn_last
-- amount
-- from fct_events_d
-- --  where dt = '20250501'
-- )
-- , summary (
-- select user_id
-- ,sum(amount)
-- ,min(event_date) min_event_date
-- ,max(event_date) max_event_date
--
--     )
--
-- select user_id,
--        case when s.first_date is null tehn t.first_date
--              when t.first_date is null then s.first_date
--             else least(s.first_date, t.first_date)
--        end first_date,
--        case when s.last_date is null then t.last_date
--              when t.last_date is null then s.last_date
--             else greatest(s.last_date, t.last_date)
--        end last_date,
--       coalse(s.amount, 0) + coalesce(t.amoujt, 0) as total_amount
--   from (select *
--            from summ table
--          where dt = yesterday) s
--        full outer join today_summary t
--        on s.user_id = t.user_id