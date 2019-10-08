 CREATE TABLE tmp.rec_like_event_orc (
    uid BIGINT,
    video_id BIGINT,
    dispatch_id VARCHAR,
    day VARCHAR,
    hour VARCHAR
 )
 WITH (                                      
    partitioned_by = ARRAY['day','hour'],
    format = 'ORC',
    location = 'hdfs://bigocluster/recommend/hive/tmp.db/rec_like_event_orc'
 )