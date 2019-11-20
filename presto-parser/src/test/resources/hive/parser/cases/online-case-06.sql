create table test1
as select t1.uid,t1.dollars,t1.10_fin_purchase
(case when t1.dollars>=5000 then 'more_5000' when t1.dollars>=1000 then '1000_5000' else '100_1000' end) level,
nvl(t2.countrycode,'unknown') country
from
(select uid,sum(a+b+c+d) dollars,max(day) 10_fin_purchase
from test2
where day between '2019-10-01' and '2019-10-31' and uid!=0
group by uid) t1 left join
test2 t2
on t1.uid=t2.uid
where t1.dollars>=100