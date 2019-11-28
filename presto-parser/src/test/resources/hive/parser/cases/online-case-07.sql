insert overwrite table report_tb.imolive_lxx_uid_country_first  partition(day)
SELECT cast(uid as BIGINT)u,upper(if(countrycode rlike '^[a-zA-Z]+$',countrycode,'UNKNOWN'))c,cast(from_unixtime(`timestamp`) as STRING)as time,'p' as tag,day
from t1 WHERE day='2019-08-01'
AND   countrycode is not null and c rlike '^[a-zA-Z]+$'  AND status=0
UNION
select cast(uid64 as BIGINT)u,upper(if(country rlike '^[a-zA-Z]+$',country,'UNKNOWN'))c, rtime as time,'q' tag,day
from  t2
WHERE   day='2019-08-01'
AND uid64!=0
and event_id='1'