select a2.day,a2.country,count(distinct a2.uid) as dau,count(distinct b2.uid) as retention_num,
count(distinct b2.uid)/cast(count(distinct a2.uid) as double) as rate
from
(
select *
from
(
select day,uid,if( event_id='02001001' ,reserve['country'] ,country)    country
from  indigo.indigo_show_user_event_orc
where day between '2019-07-24' and '2019-08-07'
AND (event_id IN('02001002',
                 '02001003',
                 '02001004',
                 '02001005',
                 '02005002',
                 '02005003',
                 '02005007',
                 '02005008')
     OR (event_id='02005004'
         AND event.event_info['type']='2')
     OR (event_id='02001001'
         AND event.log_extra['scene'] <> 'INDIGO_PERSONAL_TRENDING'))
group by day,uid,if( event_id='02001001' ,reserve['country'] ,country)
) a1
where country in( 'KW','SA','IQ')
) a2
left join
(select
*
from
(
select day,uid,if( event_id='02001001' ,reserve['country'] ,country)    country
from  indigo.indigo_show_user_event_orc
where day between '2019-07-24' and '2019-08-08'
AND (event_id IN('02001002',
                 '02001003',
                 '02001004',
                 '02001005',
                 '02005002',
                 '02005003',
                 '02005007',
                 '02005008')
     OR (event_id='02005004'
         AND event.event_info['type']='2')
     OR (event_id='02001001'
         AND event.log_extra['scene'] <> 'INDIGO_PERSONAL_TRENDING'))
group by day,uid,if( event_id='02001001' ,reserve['country'] ,country)
) b1
where country in( 'KW','SA','IQ')
) b2
on a2.day= date_sub(b2.day,1)
and a2.uid = b2.uid
group by a2.day,a2.country