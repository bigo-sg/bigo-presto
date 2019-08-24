SELECT '2019-08-22' AS sday,
if(t1.os is null, 'all', t1.os) as platform,
if(t1.client_version is null, 'all', t1.client_version) as clientversioncode,
if(t1.country is null, 'all', UPPER(t1.country)) as countrycode,
if(t1.event.event_info['endreason'] is null, 'all', t1.event.event_info['endreason']) as stop_reason,
count(*) as stop_count,
'2019-08-22' AS day
from vlog.like_user_event_orc as t1
where day = '2019-08-22'
and event_id = '050101020'
and t1.os is not null
and t1.client_version is not null
and t1.event.event_info['endreason'] is not null
and t1.country is not null and t1.country rlike '^[A-Z]+$'
group by (t1.os, t1.client_version, t1.event.event_info['endreason'],t1.country) with cube limit 20000