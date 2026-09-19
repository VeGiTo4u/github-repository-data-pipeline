-- dim_date.sql
-- Calendar Dimension

{{
    config(
        materialized='table'
    )
}}

with dates as (
    select explode(sequence(to_date('2020-01-01'), current_date(), interval 1 day)) as date_actual
)
select
    cast(date_format(date_actual, 'yyyyMMdd') as int) as date_key,
    date_actual,
    year(date_actual) as year,
    quarter(date_actual) as quarter,
    month(date_actual) as month,
    weekofyear(date_actual) as week_of_year,
    dayofweek(date_actual) as day_of_week
from dates
