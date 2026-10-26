select symbol,minute_start from {{ ref('stg_candles') }}
group by 1,2 having count(*)<>1
