create table tmp.rec_like_event_orc_1
(
 uid integer,
 sex boolean,
 age varchar,
 video_id bigint,
 day varchar,
 hour varchar
)
with
(
partitioned_by=ARRAY['day','hour'],
format = 'ORC',
external_location='hdfs://bigocluster/apps/hive/warehouse/tmp.db/rec_like_event_orc_1'
)