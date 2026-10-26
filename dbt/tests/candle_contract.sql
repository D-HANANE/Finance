select * from {{ ref('stg_candles') }}
where not (low<=open and open<=high and low<=close and close<=high)
 or volume<0 or trade_count<=0 or notional<=0
