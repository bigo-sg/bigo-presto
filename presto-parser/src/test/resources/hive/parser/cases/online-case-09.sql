select * from
(select d.t.*
from d.t3
where
day >= '2019-11-27' and day <= '2019-11-27'
and if(nn like 'mobile%', if(length(nn) > 8, substr(substr(n, 8), 0, length(nn) - 8), "unknown"), "wifi") is not null
and if(nn like 'mobile%', if(length(nn) > 8, substr(substr(nn, 8), 0, length(nn) - 8), "unknown"), "wifi") <> 'null'
and length(if(nn like 'mobile%', if(length(nn) > 8, substr(substr(nn, 8), 0, length(nn) - 8), "unknown"), "wifi")) < 10
and nt is not null
and sys is not null
and c rlike '^[A-Za-z0-9]+$'
and ver = 1538
)s1
join
(
select distinct deviceId,
    day,
    case when element_at(e.le, 'abflags_v2') like '%"178_ecdhe"%' then "178_ecdhe"
        when element_at(e.le, 'abflags_v2') like '%"178_rsa_only"%' then "178_rsa_only"
        else "not_def_flag"
        end as exp_group
from d.t1
where cast(cv as bigint) = 1538
and day >= '2019-11-27' and day <= '2019-11-27'
and (element_at(e.le, 'abflags_v2') like '%"178_ecdhe"%' or element_at(e.le, 'abflags_v2') like '%"178_rsa_only"%' )
and e in('010103001', '010106001'))s2
on s1.di = s2.di
limit 10
