package io.hivesql.sql.parser;

import org.testng.annotations.Test;

public class ShowPartitions extends SQLTester {

    @Test
    public void testShowPartitions()
    {
        String hiveSql = "show partitions db.tbl partition(day='2019-12-12', hour='01')";
        String prestoSql = "select * from db.\"tbl$partitions\" where day='2019-12-12' and hour='01'";
        checkASTNode(prestoSql, hiveSql);
    }
}
