-- One unordered pair over all common minute returns in the retained history.
select a.symbol as symbol_a,b.symbol as symbol_b,
 case when count(*)>=3 then corr(a.log_return,b.log_return) end as correlation,
 count(*) as observations
from {{ ref('minute_returns') }} a join {{ ref('minute_returns') }} b
 on a.minute_start=b.minute_start and a.symbol<b.symbol
where a.log_return is not null and b.log_return is not null group by 1,2
