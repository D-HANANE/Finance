-- Full recompute is intentional: small dataset and current UTC day changes every run.
with daily as (
 select symbol, cast(minute_start as date) as day,
 arg_max(cast(close as double),minute_start) as close,
 case when count(log_return)>=2 then stddev_samp(log_return) end as daily_volatility,
 cast(sum(notional) as double)/nullif(sum(volume),0) as vwap,
 count(*) as minutes, cast(minute_start as date)<current_date as is_complete
 from {{ ref('minute_returns') }} group by 1,2
), previous as (
 select *,lag(close) over (partition by symbol order by day) as previous_close,
 lag(day) over (partition by symbol order by day) as previous_day from daily
)
select symbol,day,close,
 case when day-previous_day=1 then close/previous_close-1 end as daily_return,
 daily_volatility,vwap,minutes,is_complete from previous
