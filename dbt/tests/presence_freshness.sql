select 1 as failure
where (select count(*) from {{ ref('stg_candles') }})=0
 or (select max(minute_end) from {{ ref('stg_candles') }}) < current_timestamp-interval '10 minutes'
