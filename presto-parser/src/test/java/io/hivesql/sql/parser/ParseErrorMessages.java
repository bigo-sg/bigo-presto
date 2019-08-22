package io.hivesql.sql.parser;

import io.prestosql.sql.parser.ParsingException;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ParseErrorMessages extends SQLTester {

    @Test
    public void testAntlr4ParseErrorMessage()
    {
        String sql = "SELECT a from b from c";

        try {
            runHiveSQL(sql);
            Assert.fail("sql: " + sql + " should throw exception");
        }catch (ParsingException e) {
            Assert.assertTrue(e.getMessage().startsWith("line 1:17: mismatched input 'from'. "));
        }
    }

    @Test
    public void testInternalParseErrorMessage()
    {
        String sql = "SELECT a from b sort by c";

        try {
            runHiveSQL(sql);
            Assert.fail("sql: " + sql + " should throw exception");
        }catch (ParsingException e) {
            Assert.assertTrue(e.getMessage().contains("Don't support sort by"));
        }
    }

    @Test
    public void testInsertOverwrite()
    {
        String sql = "" +
                "INSERT OVERWRITE TABLE algo.indigo_user_preference_by_day PARTITION (day='2019-08-20', category='label_v2') \n" +
                "SELECT uid, SUM(clicked) as click, SUM(IF(stay_time>15, 15, stay_time)) as stay_time,  SUM(IF(play_second>45, 45, play_second)) as play_second, \n" +
                "SUM(IF(complete_count>5, 5, complete_count)) as complete_count, SUM(download) as download, SUM(contact) as contact, SUM(story) as story, COUNT(1) as impression, \n" +
                "NULL, NULL, NULL, NULL, NULL, NULL, coalesce(get_json_object(substr(regexp_replace(tag, '\\\\\\\"', '\\\"'), 2, length(regexp_replace(tag, '\\\\\\\"', '\\\"'))-2), '$.bucket_priority'), '0'), NULL\n" +
                "FROM\n" +
                "(     \n" +
                "        SELECT *          \n" +
                "        FROM algo.indigo_rec_show_event_orc t1         \n" +
                "        WHERE  abflags_v3!='' and day='2019-08-20' and dispatch_id is not null and dispatch_id != ''    \n" +
                ") u \n" +
                "GROUP BY day, uid, label_v2\n" +
                "HAVING play_second > 20";

        try {
            runHiveSQL(sql);
            Assert.fail("sql: " + sql + " should throw exception");
        }catch (ParsingException e) {
            Assert.assertTrue(e.getMessage().contains("Don't support"));
        }
    }

    @Test
    public void testShowPartitions()
    {
        String sql = "Show Partitions mytable";

        try {
            runHiveSQL(sql);
            Assert.fail("sql: " + sql + " should throw exception");
        }catch (ParsingException e) {
            Assert.assertTrue(e.getMessage().contains("Don't support"));
        }
    }

    @Test
    public void testUsingUDTFFuncCall()
    {
        String sql = "" +
                "select explode(`tables`) as `tables` from bigolive.hive_job_audit\n" +
                "where day >= '2019-06-01'";

        try {
            runHiveSQL(sql);
            Assert.fail("sql: " + sql + " should throw exception");
        }catch (ParsingException e) {
            Assert.assertTrue(e.getMessage().contains("Don't Support call UDTF: explode directly, please try lateral view syntax instead."));
        }
    }



    @Test
    public void testMissingJoinCriteria()
    {
        String sql = "" +
                "select count(distinct t2.hdid) as dau,sum(video_view_time_01)/count(distinct t2.hdid)\n" +
                "from\n" +
                "right join\n" +
                "(select distinct day,hdid\n" +
                "from\n" +
                "like_dw_com.dwd_like_com_dim_snapshot_user_device\n" +
                "where day='2019-08-13'\n" +
                " ) t2\n" +
                " left join\n" +
                "(select day,hdid,sum(video_view_time_01) as video_view_time_01\n" +
                "from\n" +
                "like_dw_vvd.dwd_like_vvd_video_view_with_list\n" +
                "where day='2019-08-13'\n" +
                "group by day,hdid ) t1\n" +
                " on t1.day=t2.day and t1.hdid=t2.hdid" +
                "";

        try {
            runHiveSQL(sql);
            Assert.fail("sql: " + sql + " should throw exception");
        }catch (ParsingException e) {
            Assert.assertTrue(e.getMessage().contains("Missing join criteria"));
        }
    }

    @Test
    public void testMissingTableAlias()
    {
        String sql = "" +
                "select\n" +
                "t3.uid,t3.hdid,t3.os,t3.os_version,t3.model,t3.resolution,t3.client_version,t3.sdk_version,t3.isp,t3.net,t3.appsflyer_id,t3.day\n" +
                ",t4.gender,t4.birthday\n" +
                "from\n" +
                "(\n" +
                "  select uid,hdid,os,os_version,model,resolution,client_version,sdk_version,isp,net,appsflyer_id,day\n" +
                "  from\n" +
                "  (\n" +
                "    select uid,hdid,os,os_version,model,resolution,client_version,sdk_version,isp,net,appsflyer_id,day\n" +
                "    --,row_number()over (partition by uid order by day desc) as rank_over\n" +
                "    from like_dw_com.dwd_like_com_dim_snapshot_user_device\n" +
                "    where country = 'IN'\n" +
                " -- 这个就是最近的一天的值\n" +
                " and day = '2019-08-12'\n" +
                "  )\n" +
                "  --where rank_over=1\n" +
                ") t3\n" +
                "join\n" +
                "(\n" +
                "  SELECT t1.uid,t1.gender,t1.birthday\n" +
                "  from like_dw_com.dwd_like_com_dim_uid_basic_info t1\n" +
                "  join tmp.guoyanyan_0813_only_push t2\n" +
                "  on t1.uid = t2.uid\n" +
                ") t4\n" +
                "on t3.uid = t4.uid" +
                "";

        try {
            runHiveSQL(sql);
            Assert.fail("sql: " + sql + " should throw exception");
        }catch (ParsingException e) {
            Assert.assertTrue(e.getMessage().contains("Missing table alias"));
        }
    }
}
