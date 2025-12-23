{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'Date_id',
    merge_exclude_columns = ['Insert_date']
) }}

with distinct_dates as (
    select distinct
        DATE,
        ISHOLIDAY
    from {{ source('walmart_raw', 'dept_raw') }}
)

select
    to_number(to_char(DATE, 'YYYYMMDD')) as Date_id,
    DATE as Store_Date,
    cast(ISHOLIDAY as varchar) as Isholiday,
    current_timestamp() as Insert_date,
    current_timestamp() as Update_date
from distinct_dates