package io.prestosql.sql.rewrite;

import io.prestosql.Session;
import io.prestosql.SystemSessionProperties;
import io.prestosql.metadata.SessionPropertyManager;
import io.prestosql.spi.QueryId;
import io.prestosql.spi.security.Identity;
import io.prestosql.sql.parser.ParsingOptions;
import io.prestosql.sql.parser.SqlParser;
import io.prestosql.sql.tree.Statement;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Optional;

public class DownloadRewriteTest {
    private static SqlParser sqlParser = new SqlParser();
    private static ParsingOptions prestoParsingOptions = new ParsingOptions(ParsingOptions.DecimalLiteralTreatment.AS_DECIMAL);

    static {
        prestoParsingOptions.setIfUseHiveParser(false);
    }

    protected Statement createStatement(String sql) {
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

        Statement rewriteNode = rewrite.rewrite(session, null, sqlParser, null, createStatement(originalSQL), null, null, null);

        Assert.assertEquals(rewriteNode, createStatement(expectedSQL));

        // make sure the session properties is set correctly.
        Assert.assertEquals(SystemSessionProperties.isScaleWriters(session), false);
        Assert.assertEquals(SystemSessionProperties.isRedistributeWrites(session), false);
    }

    void testRewrite(String sql) {
        testRewrite(sql, CATS_PREFIX + sql + LIMIT_SUFFIX);
    }

    void testRewrite(String originalSQL, String expectedSQL) {
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

    /////////////
    static String CATS_PREFIX = "" +
            "CREATE TABLE download_db." + DownloadRewrite.RESULT_TABLE_NAME_PREFIX + "query_123\n" +
            "COMMENT '" + DownloadRewrite.COMMENT.get() + "'\n" +
            "WITH (format = 'RCBINARY')\n" +
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
}