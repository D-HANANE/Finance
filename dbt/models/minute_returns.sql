{{ config(materialized='view') }}
with prices as (
 select *, lag(close) over (partition by symbol order by minute_start) as prev_close,
 lag(minute_start) over (partition by symbol order by minute_start) as prev_minute
 from {{ ref('stg_candles') }}
)
select *, case when minute_start-prev_minute=interval '1 minute'
 then ln(cast(close as double)/cast(prev_close as double)) end as log_return
from prices
