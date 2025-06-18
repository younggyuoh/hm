{{ config(
    materialized='table'
) }}

-- In a production environment, this table would be updated incrementally on a daily basis.
-- For this small-scale assignment, partitioning was omitted for simplicity.

select cast(date_trunc('day', event_time) as text) as event_date_utc,
       cast(event_time as timestamp) as event_time_utc,
       coalesce(user_id,'null') as user_id,
       coalesce(event_type,'null') as event_type,
       coalesce(transaction_category,'null') as transaction_category,
       cast(miles_amount as double) as miles_amount,
       coalesce(platform,'null') as platform,
       coalesce(utm_source,'null') as utm_source,
       coalesce(country,'null') as country,
       current_timestamp as etl_at_utc,

  from {{ source('manual_loaded','raw_events_d') }}
-- where dt = '{{ ds_nodash }}'  (partition pruning - 20250614)
