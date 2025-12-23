{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = ['Store_id', 'Dept_id'],
    merge_exclude_columns = ['Insert_date']
) }}

with store_raw_data as (
    select 
        STORE,
        TYPE,
        SIZE
    from {{ source('walmart_raw', 'stores_raw') }}
),

dept_raw_data as (
    -- We need distinct Store-Dept combinations to build the dimension
    select distinct
        STORE,
        DEPT
    from {{ source('walmart_raw', 'dept_raw') }}
)

select
    s.STORE as Store_id,
    d.DEPT as Dept_id,
    s.TYPE as Store_type,
    s.SIZE as Store_size,
    current_timestamp() as Insert_date,
    current_timestamp() as Update_date
from store_raw_data s
join dept_raw_data d on s.STORE = d.STORE