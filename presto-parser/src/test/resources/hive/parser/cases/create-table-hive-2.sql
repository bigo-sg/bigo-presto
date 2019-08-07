CREATE TABLE `tmp.rec_like_event_orc`(
<<<<<<< HEAD
  `uid` bigint)
=======
  `uid` bigint, 
  `video_id` bigint, 
  `dispatch_id` string, 
  `refer` string, 
  `dispatched` int, 
  `displayed` int, 
  `clicked` int, 
  `slide_play` int, 
  `followed` int, 
  `liked` int, 
  `shared` int, 
  `comments` int, 
  `slide` int, 
  `stay` int, 
  `play_second` int, 
  `complete_count` int, 
  `update_timestamp` string, 
  `slide_time` int, 
  `event_time` string, 
  `req_pos` int, 
  `ranker` string, 
  `rough_ranker` string, 
  `score` string, 
  `rough_ranker_score` string, 
  `selector` string, 
  `strategy` string, 
  `check_status` string, 
  `cover_ab` string, 
  `plugin` string, 
  `domain_type` string, 
  `domain_value` string, 
  `abflags_v3` string, 
  `user_type` string, 
  `filter` string, 
  `os` string, 
  `country` string, 
  `country_region` string, 
  `lng` bigint, 
  `lat` bigint, 
  `net` string, 
  `list_pos` int, 
  `ob1` string, 
  `ob2` string, 
  `ob3` string, 
  `ob4` string, 
  `ob5` string)
>>>>>>> support create table as select & support insert into
PARTITIONED BY ( 
  `day` string, 
  `hour` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  'hdfs://bigocluster/recommend/hive/tmp.db/rec_like_event_orc'
TBLPROPERTIES (
  'last_modified_by'='tangyun', 
  'last_modified_time'='1544684612', 
  'orc.compress'='SNAPPY', 
  'transient_lastDdlTime'='1554739798')
