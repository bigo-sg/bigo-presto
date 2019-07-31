WITH gpu_stats AS
  (SELECT rip,
          DAY,
          FROM_UNIXTIME(CAST (`timestamp` AS BIGINT), 'yyyy-MM-dd HH:mm') AS MINUTE,
          SUM(codecstreamcount) AS codecstreamcount,
          SUM(usercount) AS usercount,
          AVG(decoderutilization) AS decoderutil,
          AVG(encoderutilization) AS encoderutil,
          AVG(memused) AS memused,
          AVG(memfree) AS memfree,
          AVG(memtotal) AS memtotal
   FROM cubetv.cube_cs_load_stats
   WHERE FROM_UNIXTIME(CAST (`timestamp` AS BIGINT), 'yyyy-MM-dd HH')='2019-07-30 12'
     AND DAY='2019-07-30'
   GROUP BY rip,
            DAY,
            FROM_UNIXTIME(CAST (`timestamp` AS BIGINT), 'yyyy-MM-dd HH:mm')),
     coderate_exploded AS
  (SELECT rip,
          DAY,
          FROM_UNIXTIME(CAST (`timestamp` AS BIGINT), 'yyyy-MM-dd HH:mm') AS MINUTE,
          transcodeuserinfos.userinfo
   FROM cubetv.cube_cs_load_stats LATERAL VIEW EXPLODE(transcodeuserinfo) transcodeuserinfos AS userinfo
   WHERE FROM_UNIXTIME(CAST (`timestamp` AS BIGINT), 'yyyy-MM-dd HH')='2019-07-30 12'
     AND DAY='2019-07-30')
INSERT overwrite TABLE report_tb.cs_stats_detail_lizhengtang_20181013 partition (DAY)
SELECT gpu_stats.day AS sday,
       gpu_stats.minute AS MINUTE,
       gpu_stats.rip AS rip,
       getjifang(gpu_stats.rip) AS jifang,
       gpu_stats.codecstreamcount AS codecstreamcount,
       gpu_stats.usercount AS usercount,
       gpu_stats.encoderutil AS encoderutil,
       gpu_stats.decoderutil AS decoderutil,
       gpu_stats.memused AS memused,
       gpu_stats.memfree AS memfree,
       gpu_stats.memtotal AS memtotal,
       CASE
           WHEN coderate_stats.in_obs_unknown IS NULL THEN 0
           ELSE coderate_stats.in_obs_unknown
       END AS in_obs_unknown,
       CASE
           WHEN coderate_stats.in_obs_1080p60 IS NULL THEN 0
           ELSE coderate_stats.in_obs_1080p60
       END AS in_obs_1080p60,
       CASE
           WHEN coderate_stats.in_obs_1080p30 IS NULL THEN 0
           ELSE coderate_stats.in_obs_1080p30
       END AS in_obs_1080p30,
       CASE
           WHEN coderate_stats.in_obs_720p60 IS NULL THEN 0
           ELSE coderate_stats.in_obs_720p60
       END AS in_obs_720p60,
       CASE
           WHEN coderate_stats.in_obs_720p30 IS NULL THEN 0
           ELSE coderate_stats.in_obs_720p30
       END AS in_obs_720p30,
       CASE
           WHEN coderate_stats.in_obs_480p60 IS NULL THEN 0
           ELSE coderate_stats.in_obs_480p60
       END AS in_obs_480p60,
       CASE
           WHEN coderate_stats.in_obs_480p30 IS NULL THEN 0
           ELSE coderate_stats.in_obs_480p30
       END AS in_obs_480p30,
       CASE
           WHEN coderate_stats.in_obs_360p30 IS NULL THEN 0
           ELSE coderate_stats.in_obs_360p30
       END AS in_obs_360p30,
       CASE
           WHEN coderate_stats.in_obs_240p30 IS NULL THEN 0
           ELSE coderate_stats.in_obs_240p30
       END AS in_obs_240p30,
       CASE
           WHEN coderate_stats.in_unknown IS NULL THEN 0
           ELSE coderate_stats.in_unknown
       END AS in_unknown,
       CASE
           WHEN coderate_stats.in_1080p60 IS NULL THEN 0
           ELSE coderate_stats.in_1080p60
       END AS in_1080p60,
       CASE
           WHEN coderate_stats.in_1080p30 IS NULL THEN 0
           ELSE coderate_stats.in_1080p30
       END AS in_1080p30,
       CASE
           WHEN coderate_stats.in_720p60 IS NULL THEN 0
           ELSE coderate_stats.in_720p60
       END AS in_720p60,
       CASE
           WHEN coderate_stats.in_720p30 IS NULL THEN 0
           ELSE coderate_stats.in_720p30
       END AS in_720p30,
       CASE
           WHEN coderate_stats.in_480p60 IS NULL THEN 0
           ELSE coderate_stats.in_480p60
       END AS in_480p60,
       CASE
           WHEN coderate_stats.in_480p30 IS NULL THEN 0
           ELSE coderate_stats.in_480p30
       END AS in_480p30,
       CASE
           WHEN coderate_stats.in_360p30 IS NULL THEN 0
           ELSE coderate_stats.in_360p30
       END AS in_360p30,
       CASE
           WHEN coderate_stats.in_240p30 IS NULL THEN 0
           ELSE coderate_stats.in_240p30
       END AS in_240p30,
       CASE
           WHEN coderate_stats.out_1080p60 IS NULL THEN 0
           ELSE coderate_stats.out_1080p60
       END AS out_1080p60,
       CASE
           WHEN coderate_stats.out_1080p30 IS NULL THEN 0
           ELSE coderate_stats.out_1080p30
       END AS out_1080p30,
       CASE
           WHEN coderate_stats.out_720p60 IS NULL THEN 0
           ELSE coderate_stats.out_720p60
       END AS out_720p60,
       CASE
           WHEN coderate_stats.out_720p30 IS NULL THEN 0
           ELSE coderate_stats.out_720p30
       END AS out_720p30,
       CASE
           WHEN coderate_stats.out_480p60 IS NULL THEN 0
           ELSE coderate_stats.out_480p60
       END AS out_480p60,
       CASE
           WHEN coderate_stats.out_480p30 IS NULL THEN 0
           ELSE coderate_stats.out_480p30
       END AS out_480p30,
       CASE
           WHEN coderate_stats.out_360p30 IS NULL THEN 0
           ELSE coderate_stats.out_360p30
       END AS out_360p30,
       CASE
           WHEN coderate_stats.out_240p30 IS NULL THEN 0
           ELSE coderate_stats.out_240p30
       END AS out_240p30,
       getregion(gpu_stats.rip) AS jifang2,
       gpu_stats.day AS DAY
FROM gpu_stats
LEFT JOIN
  (SELECT rip,
          MINUTE,
          sum(CLASSIFY('in_obs_unknown', userinfo.videoquality, userinfo.coderate)) AS in_obs_unknown,
          sum(CLASSIFY('in_obs_1080p60', userinfo.videoquality, userinfo.coderate)) AS in_obs_1080p60,
          sum(CLASSIFY('in_obs_1080p30', userinfo.videoquality, userinfo.coderate)) AS in_obs_1080p30,
          sum(CLASSIFY('in_obs_720p60', userinfo.videoquality, userinfo.coderate)) AS in_obs_720p60,
          sum(CLASSIFY('in_obs_720p30', userinfo.videoquality, userinfo.coderate)) AS in_obs_720p30,
          sum(CLASSIFY('in_obs_480p60', userinfo.videoquality, userinfo.coderate)) AS in_obs_480p60,
          sum(CLASSIFY('in_obs_480p30', userinfo.videoquality, userinfo.coderate)) AS in_obs_480p30,
          sum(CLASSIFY('in_obs_360p30', userinfo.videoquality, userinfo.coderate)) AS in_obs_360p30,
          sum(CLASSIFY('in_obs_240p30', userinfo.videoquality, userinfo.coderate)) AS in_obs_240p30,
          sum(CLASSIFY('in_unknown', userinfo.videoquality, userinfo.coderate)) AS in_unknown,
          sum(CLASSIFY('in_1080p60', userinfo.videoquality, userinfo.coderate)) AS in_1080p60,
          sum(CLASSIFY('in_1080p30', userinfo.videoquality, userinfo.coderate)) AS in_1080p30,
          sum(CLASSIFY('in_720p60', userinfo.videoquality, userinfo.coderate)) AS in_720p60,
          sum(CLASSIFY('in_720p30', userinfo.videoquality, userinfo.coderate)) AS in_720p30,
          sum(CLASSIFY('in_480p60', userinfo.videoquality, userinfo.coderate)) AS in_480p60,
          sum(CLASSIFY('in_480p30', userinfo.videoquality, userinfo.coderate)) AS in_480p30,
          sum(CLASSIFY('in_360p30', userinfo.videoquality, userinfo.coderate)) AS in_360p30,
          sum(CLASSIFY('in_240p30', userinfo.videoquality, userinfo.coderate)) AS in_240p30,
          sum(CLASSIFY('out_1080p60', userinfo.videoquality, userinfo.coderate)) AS out_1080p60,
          sum(CLASSIFY('out_1080p30', userinfo.videoquality, userinfo.coderate)) AS out_1080p30,
          sum(CLASSIFY('out_720p60', userinfo.videoquality, userinfo.coderate)) AS out_720p60,
          sum(CLASSIFY('out_720p30', userinfo.videoquality, userinfo.coderate)) AS out_720p30,
          sum(CLASSIFY('out_480p60', userinfo.videoquality, userinfo.coderate)) AS out_480p60,
          sum(CLASSIFY('out_480p30', userinfo.videoquality, userinfo.coderate)) AS out_480p30,
          sum(CLASSIFY('out_360p30', userinfo.videoquality, userinfo.coderate)) AS out_360p30,
          sum(CLASSIFY('out_240p30', userinfo.videoquality, userinfo.coderate)) AS out_240p30
   FROM coderate_exploded
   GROUP BY rip,
            DAY,
            MINUTE)coderate_stats ON gpu_stats.rip = coderate_stats.rip
AND gpu_stats.minute = coderate_stats.minute