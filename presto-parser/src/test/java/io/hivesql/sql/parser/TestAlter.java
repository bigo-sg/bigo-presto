package io.hivesql.sql.parser;

import org.testng.annotations.Test;

/**
 * @author tangyun@bigo.sg
 * @date 8/13/19 3:14 PM
 */
public class TestAlter extends SQLTester {

    @Test
    public void testAlterDropPartition01() {
        String prestoSql = "delete from tbl where day = '2019-05-01' and hour = '01'";
        String hiveSql = "ALTER TABLE tbl DROP IF EXISTS PARTITION (day = '2019-05-01', hour='01')";
        checkASTNode(prestoSql, hiveSql);
    }
    @Test
    public void testAlterDropPartition02() {
        String prestoSql = "delete from tbl where (day = '2019-05-01' and hour = '01') or (day = '2019-05-01' and hour = '02')";
        String hiveSql = "ALTER TABLE tbl DROP IF EXISTS PARTITION (day = '2019-05-01', hour='01'),PARTITION (day = '2019-05-01', hour='02')";
        checkASTNode(prestoSql, hiveSql);
    }
}
