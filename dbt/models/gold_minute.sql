-- One finalized symbol-minute. 20/50 observed one-minute closes, NULL until full history.
select symbol, minute_start, cast(close as double) as close,
 case when count(*) over w20 = 20 then avg(close) over w20 end as ma20,
 case when count(*) over w50 = 50 then avg(close) over w50 end as ma50
from {{ ref('stg_candles') }}
window w20 as (partition by symbol order by minute_start rows between 19 preceding and current row),
       w50 as (partition by symbol order by minute_start rows between 49 preceding and current row)
