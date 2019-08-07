create table tmp.rec_like_event_orc_1
(
<<<<<<< HEAD
 uid INTEGER ,
 sex BOOLEAN,
 age VARCHAR ,
 video_id BIGINT,
 day VARCHAR,
 hour VARCHAR
=======
 uid integer,
 sex boolean,
 age varchar,
 video_id bigint,
 day varchar,
 hour varchar
>>>>>>> support create table as select & support insert into
)
with
(
partitioned_by=ARRAY['day','hour'],
format = 'ORC',
external_location='hdfs://bigocluster/apps/hive/warehouse/tmp.db/rec_like_event_orc_1'
)