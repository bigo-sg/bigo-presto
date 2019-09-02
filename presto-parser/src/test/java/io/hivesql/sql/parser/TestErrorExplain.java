package io.hivesql.sql.parser;

import io.prestosql.sql.parser.ParsingException;
import org.testng.annotations.Test;

public class TestErrorExplain extends SQLTester {

    @Test
    public void test01() {
        String sql = "select a from t where m 'sdffs'";
        try {
            runHiveSQL(sql);
        } catch (ParsingException e) {

        }
    }
    @Test
    public void test02() {
        String sql = "select a from t where between 'sdffs' and 'sad'";
        try {
            runHiveSQL(sql);
        } catch (ParsingException e) {

        }
    }
    @Test
    public void test03() {
        String sql = "select a from t where like 'sdffs'";
        try {
            runHiveSQL(sql);
        } catch (ParsingException e) {

        }
    }

    @Test
    public void testNested01()
    {
        String hiveSql = "select user FROM ((select user from tbl))";
        try {
            runHiveSQL(hiveSql);
        } catch (ParsingException e) {

        }
    }

    @Test
    public void testNested02()
    {
        String hiveSql = "select user FROM ((select user,id from tbl left join tbl1))";
        try {
            runHiveSQL(hiveSql);
        } catch (ParsingException e) {

        }
    }
}
