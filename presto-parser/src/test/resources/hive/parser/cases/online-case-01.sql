select
t3.uid,t3.hdid,t3.os,t3.os_version,t3.model,t3.resolution,t3.client_version,t3.sdk_version,t3.isp,t3.net,t3.appsflyer_id,t3.day
,t4.gender,t4.birthday
from
(
  select uid,hdid,os,os_version,model,resolution,client_version,sdk_version,isp,net,appsflyer_id,day
  from
  (
    select uid,hdid,os,os_version,model,resolution,client_version,sdk_version,isp,net,appsflyer_id,day
    --,row_number()over (partition by uid order by day desc) as rank_over
    from like_dw_com.dwd_like_com_dim_snapshot_user_device
    where country = 'IN'
 -- 这个就是最近的一天的值
  and day = '2019-08-12'
  )m
  --where rank_over=1
) t3
join
(
  SELECT t1.uid,t1.gender,t1.birthday
  from like_dw_com.dwd_like_com_dim_uid_basic_info t1
  join tmp.guoyanyan_0813_only_push t2
  on t1.uid = t2.uid
) t4
on t3.uid = t4.uid limit 200000