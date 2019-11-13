package io.prestosql.sql.rewrite;

import io.prestosql.Session;
import io.prestosql.SystemSessionProperties;
import io.prestosql.metadata.SessionPropertyManager;
import io.prestosql.spi.QueryId;
import io.prestosql.spi.security.Identity;
import io.prestosql.sql.parser.ParsingException;
import io.prestosql.sql.parser.ParsingOptions;
import io.prestosql.sql.parser.SqlParser;
import io.prestosql.sql.tree.Statement;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Optional;

public class DownloadRewriteTest {
    private static SqlParser sqlParser = new SqlParser();
    private static ParsingOptions prestoParsingOptions = new ParsingOptions(ParsingOptions.DecimalLiteralTreatment.AS_DECIMAL);
    private static ParsingOptions hiveParsingOptions = new ParsingOptions(ParsingOptions.DecimalLiteralTreatment.AS_DECIMAL);

    static {
        prestoParsingOptions.setIfUseHiveParser(false);
        hiveParsingOptions.setIfUseHiveParser(true);
    }

    protected Statement createStatement(String sql) {
        try {
            return usePrestoParser(sql);
        } catch (ParsingException e) {
            // if failed, try using hive syntax.
            return useHiveParser(sql);
        }
    }

    protected Statement useHiveParser(String sql) {
        return sqlParser.createStatement(sql, hiveParsingOptions);
    }
    protected Statement usePrestoParser(String sql) {
        return sqlParser.createStatement(sql, prestoParsingOptions);
    }

    DownloadRewrite rewrite = new DownloadRewrite();

    Session getSession(boolean enableDownloadRewrite) {
        SessionPropertyManager sessionPropertyManager = new SessionPropertyManager();
        Session.SessionBuilder sessionBuilder = Session.builder(sessionPropertyManager);

        sessionBuilder.setQueryId(new QueryId("query_123"));
        sessionBuilder.setIdentity(new Identity("identity", Optional.empty()));

        sessionBuilder.setSystemProperty(SystemSessionProperties.ENABLE_DOWNLOAD_REWRITE, String.valueOf(enableDownloadRewrite));
        sessionBuilder.setSystemProperty(SystemSessionProperties.DOWNLOAD_REWRITE_DB_NAME, "download_db");
        sessionBuilder.setSystemProperty(SystemSessionProperties.DOWNLOAD_REWRITE_ROW_LIMIT, "988");

        // this help us to double check the session properties is set correctly after rewrite.
        sessionBuilder.setSystemProperty(SystemSessionProperties.SCALE_WRITERS, "true");
        sessionBuilder.setSystemProperty(SystemSessionProperties.REDISTRIBUTE_WRITES, "true");

        return sessionBuilder.build();
    }

    void testRewrite(String originalSQL, String expectedSQL, boolean enableDownloadRewrite) {
        Session session = getSession(enableDownloadRewrite);

        Statement rewriteNode = rewrite.rewrite(session, null, sqlParser, null, createStatement(originalSQL), null, null, null, null);

        Assert.assertEquals(rewriteNode, createStatement(expectedSQL));

        if (!originalSQL.equals(expectedSQL)) {
            // make sure the session properties is set correctly.
            Assert.assertEquals(SystemSessionProperties.isScaleWriters(session), false);
            Assert.assertEquals(SystemSessionProperties.isRedistributeWrites(session), false);
        }
    }

    void testRewrite(String sql) {
        testRewrite(sql, CATS_PREFIX + sql + LIMIT_SUFFIX);
    }

    void testRewriteUsingHive(String sql) {
        testRewrite(sql, HIVE_CATS_PREFIX + sql + LIMIT_SUFFIX);
    }

    void testRewrite(String originalSQL, String expectedSQL) {
        testRewrite(originalSQL, expectedSQL, true);
    }

    void testRewriteUsingHive(String originalSQL, String expectedSQL) {
        testRewrite(originalSQL, expectedSQL, true);
    }

    @Test
    public void testDownloadRewriteDisabled() {
        String sql = "" +
                "SELECT * \n" +
                "from vlog.like_user_event_hour_orc \n" +
                "where day = '2019-06-14'";

        testRewrite(sql, sql, false);
    }

    //////////////////////

    @Test
    public void testInsertStatement() {
        String sql = "" +
                "INSERT INTO orders SELECT * \n" +
                "from vlog.like_user_event_hour_orc \n" +
                "where day = '2019-06-14'";

        testRewrite(sql, sql);
    }

    @Test
    public void testCreateTableAs() {
        String sql = "" +
                "CREATE TABLE orders AS SELECT * \n" +
                "from vlog.like_user_event_hour_orc \n" +
                "where day = '2019-06-14'";

        testRewrite(sql, sql);
    }

    @Test
    public void testSimpleSelectStatementWithLimit() {
        String sql = "" +
                "SELECT * \n" +
                "from vlog.like_user_event_hour_orc \n" +
                "where day = '2019-06-14'" +
                "limit 100";

        testRewrite(sql, CATS_PREFIX + sql);
    }

    /////////////
    static String CATS_PREFIX = "" +
            "CREATE TABLE download_db." + DownloadRewrite.RESULT_TABLE_NAME_PREFIX + "query_123\n" +
            "COMMENT '" + DownloadRewrite.COMMENT.get() + "'\n" +
            "WITH (format = 'RCBINARY')\n" +
            "AS \n";

    static String HIVE_CATS_PREFIX = "" +
            "CREATE TABLE download_db." + DownloadRewrite.RESULT_TABLE_NAME_PREFIX + "query_123\n" +
            "COMMENT \"" + DownloadRewrite.COMMENT.get() + "\"\n" +
            "STORED AS RCFile\n" +
            "AS \n";

    static String LIMIT_SUFFIX = "\n limit 988";

    @Test
    public void testSimpleSelectStatement() {
        String sql = "" +
                "SELECT * \n" +
                "from vlog.like_user_event_hour_orc \n" +
                "where day = '2019-06-14'";

        testRewrite(sql);
    }

    @Test
    public void testComplexSelectStatementUsingHive() {
        String sql = "" +
                "SELECT   COALESCE(new_flag,'all') flag, \n" +
                "         count(DISTINCTIF(1st_withdraw_apply=1,uid,NULL)) 1st_withdraw_apply_cnt, \n" +
                "         count(DISTINCT IF(1st_withdraw_sus=1,uid,NULL))   1st_withdraw_sus_cnt \n" +
                "FROM     ( \n" +
                "                   SELECT    a.sday                      sday, \n" +
                "                             a.uid                       uid, \n" +
                "                             COALESCE(new_flag,'unknow') new_flag, \n" +
                "                             IF(a.withdraw_apply_cnt>0 \n" +
                "                   AND       b.withdraw_apply_cnt=0,1,0) AS 1st_withdraw_apply, \n" +
                "                             IF(a.withdraw_suc_cnt>0 \n" +
                "                   AND       b.withdraw_suc_cnt=0,1,0) AS 1st_withdraw_sus \n" +
                "                   FROM      ( \n" +
                "                                    SELECT sday, \n" +
                "                                           uid, \n" +
                "                                           new_flag, \n" +
                "                                           withdraw_apply_cnt, \n" +
                "                                           withdraw_suc_cnt \n" +
                "                                    FROM   mediate_tb.alite_gold_coin_uid_changelog_daily \n" +
                "                                    WHERE  day BETWEEN date_sub('2019-08-26',29) AND    '2019-08-26' )a \n" +
                "                   LEFT JOIN \n" +
                "                             ( \n" +
                "                                    SELECT sday, \n" +
                "                                           uid, \n" +
                "                                           withdraw_apply_cnt, \n" +
                "                                           withdraw_suc_cnt \n" +
                "                                    FROM   mediate_tb.alite_gold_coin_uid_changelog_acc \n" +
                "                                    WHERE  day BETWEEN date_sub('2019-08-26',30) AND    date_sub('2019-08-26',1) )b \n" +
                "                   ON        date_sub(a.sday,1)=b.sday \n" +
                "                   GROUP BY  a.sday , \n" +
                "                             a.uid , \n" +
                "                             new_flag, \n" +
                "                             IF(a.withdraw_apply_cnt>0 \n" +
                "                   AND       b.withdraw_apply_cnt=0,1,0), \n" +
                "                             IF(a.withdraw_suc_cnt>0 \n" +
                "                   AND       b.withdraw_suc_cnt=0,1,0) )t \n" +
                "GROUP BY new_flag WITH cube";

        testRewriteUsingHive(sql);
    }

    @Test
    public void testWithStatement() {
        String sql = "" +
                "WITH\n" +
                "  x AS (SELECT a FROM t),\n" +
                "  y AS (SELECT a AS b FROM x),\n" +
                "  z AS (SELECT b AS c FROM y)\n" +
                "SELECT c FROM z";

        testRewrite(sql);
    }

    @Test
    public void testValueStatement() {
        String sql = "SELECT * FROM (VALUES 5, 2, 4, 1, 3)";

        testRewrite(sql);
    }

    @Test
    public void testExceptStatement() {
        String sql = "" +
                "SELECT * FROM (VALUES 13, 42)\n" +
                "EXCEPT\n" +
                "SELECT 13";

        testRewrite(sql);
    }

    @Test
    public void testIntersectStatement() {
        String sql = "" +
                "SELECT * FROM (VALUES 13, 42)\n" +
                "INTERSECT\n" +
                "SELECT 13";

        testRewrite(sql);
    }

    @Test
    public void testIntersectStatementWithLimit() {
        String originalSQL = "" +
                "SELECT * FROM (VALUES 13, 42)\n" +
                "INTERSECT\n" +
                "SELECT 13\n" +
                "limit 10";

        // we should give each column a name.
        String expectedSQL = CATS_PREFIX + originalSQL;

        testRewrite(originalSQL, expectedSQL);
    }


    @Test
    public void testUnion() {
        String sql = "" +
                "SELECT origin_state, NULL, NULL, sum(package_weight)\n" +
                "FROM shipping GROUP BY origin_state\n" +
                "\n" +
                "UNION ALL\n" +
                "\n" +
                "SELECT origin_state, origin_zip, NULL, sum(package_weight)\n" +
                "FROM shipping GROUP BY origin_state, origin_zip";

        testRewrite(sql);
    }

    @Test
    public void testUnionStatement() {
        String sql = "" +
                "select distinct t3.uid from(\n" +
                "select * from(\n" +
                "select msg,count(*) as num,count(distinct bgid) as bg_num,count(distinct uid) as uid_num\n" +
                "from indigo_us.bigoreview_big_group_chat where day='2019-08-21' and length(msg)>50 and strpos(msg,'http')=0\n" +
                "group by msg\n" +
                ")t1\n" +
                "where t1.bg_num<=2 and uid_num<=2\n" +
                "and num>100\n" +
                ")t2\n" +
                "join (select * from indigo_us.bigoreview_big_group_chat where day='2019-08-21')t3\n" +
                "on t2.msg=t3.msg\n" +
                "union all\n" +
                "select distinct t3.uid from(\n" +
                "select *,length(msg) from(\n" +
                "select msg,count(*) as num,count(distinct bgid) as bg_num,count(distinct uid) as uid_num\n" +
                "from indigo_us.bigoreview_big_group_chat where day='2019-08-25' and strpos(msg,'http')>0 and length(msg)>50 \n" +
                "group by msg\n" +
                ")t\n" +
                "where num>100\n" +
                ")t2\n" +
                "join (select * from indigo_us.bigoreview_big_group_chat where day='2019-08-25')t3\n" +
                "on t2.msg=t3.msg\n" +
                "union all\n" +
                "select  t1.uid from(\n" +
                "select t.uid,all_num from(\n" +
                "select uid,count(*) as all_num,count(distinct bgid) as bg_num,count(msg) as msg_num\n" +
                "from indigo_us.bigoreview_big_group_chat where day='2019-08-25' and length(msg)>10\n" +
                "group by uid\n" +
                ")t\n" +
                "where t.bg_num>5\n" +
                "order by t.all_num\n" +
                "desc\n" +
                "limit 50\n" +
                ")t1";

        testRewrite(sql);
    }
}