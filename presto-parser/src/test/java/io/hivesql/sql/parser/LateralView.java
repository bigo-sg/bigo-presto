package io.hivesql.sql.parser;

import org.testng.annotations.Test;

public class LateralView extends SQLTester {
    @Test
    public void testLateralView()
    {
        String prestoSql = "" +
                "SELECT event.*\n" +
                "FROM tb1 CROSS JOIN UNNEST(events) AS event";
        String hiveSql = "" +
                "SELECT event.*\n" +
                "FROM tb1 lateral view explode(events) t1 as event";

        checkASTNode(prestoSql, hiveSql);
    }

    @Test
    public void testLateralViewWithOrdinality()
    {
        String prestoSql = "" +
                "SELECT event.*\n" +
                "FROM tb1 CROSS JOIN UNNEST(events) WITH ORDINALITY AS event (pos, event)";
        String hiveSql = "" +
                "SELECT event.*\n" +
                "FROM tb1 lateral view posexplode(events) t1 as pos, event";

        checkASTNode(prestoSql, hiveSql);
    }
}
