{% snapshot walmart_fact_snapshot %}

{{
    config(
      target_database='WALMART_DB',
      target_schema='ANALYTICS',
      unique_key="store_id || '-' || dept_id || '-' || date_id",
      strategy='check',
      check_cols=['store_weekly_sales', 'fuel_price', 'temperature', 'unemployment', 'cpi', 'markdown1', 'markdown2', 'markdown3', 'markdown4', 'markdown5'],
    )
}}

with fact_with_dims as (
    select
        s_dim.Store_id,
        s_dim.Dept_id,
        d_dim.Date_id,
        d_dim.Store_Date as date,
        s_dim.Store_size,
        dept.Weekly_Sales as store_weekly_sales,
        src.fuel_price,
        src.temperature,
        src.unemployment,
        src.cpi,
        src.markdown1,
        src.markdown2,
        src.markdown3,
        src.markdown4,
        src.markdown5
    from {{ source('walmart_raw', 'fact_raw') }} src
    join {{ ref('walmart_date_dim') }} d_dim 
        on src.date = d_dim.Store_Date
    join {{ ref('walmart_store_dim') }} s_dim 
        on src.store = s_dim.Store_id
    join {{ source('walmart_raw', 'dept_raw') }} dept
        on src.store = dept.store 
        and src.date = dept.date 
        and s_dim.Dept_id = dept.dept
)

select * from fact_with_dims

{% endsnapshot %}