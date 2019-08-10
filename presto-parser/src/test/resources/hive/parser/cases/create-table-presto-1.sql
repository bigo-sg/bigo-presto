create table tmp.rec_like_event_orc_1
(
 uid INTEGER ,
 sex BOOLEAN,
 age VARCHAR ,
 video_id BIGINT,
 day VARCHAR,
 hour VARCHAR
)
with
(
partitioned_by=ARRAY['day','hour'],
format = 'ORC',
external_location='hdfs://bigocluster/apps/hive/warehouse/tmp.db/rec_like_event_orc_1'
)