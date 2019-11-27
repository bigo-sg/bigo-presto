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
    public void testCrossJoinUnnestInHiveMode()
    {
        String sql = "" +
                "select  day,hour,count(distinct a.UID) as uv\n" +
                "from\n" +
                "(\n" +
                "SELECT day,hour(rtime) as hour,uid1 as uid\n" +
                "from vlog.like_online_user_uversion_stat_platform\n" +
                "cross join unnest(online) as ont\n" +
                "cross join unnest(ont.uids) as ut  (uid1)\n" +
                "where day>='2019-08-13'\n" +
                "and status=0\n" +
                ") a\n" +
                "join\n" +
                "(\n" +
                "  select UID\n" +
                "  from vlog.user_countrycode\n" +
                "  where countrycode='BD'\n" +
                ")b\n" +
                "on a.uid=b.uid\n" +
                "group by day,hour" +
                "";

        try {
            runHiveSQL(sql);
            Assert.fail("sql: " + sql + " should throw exception");
        }catch (ParsingException e) {
            Assert.assertTrue(e.getMessage().contains("Don't support unnest"));
        }
    }
}
