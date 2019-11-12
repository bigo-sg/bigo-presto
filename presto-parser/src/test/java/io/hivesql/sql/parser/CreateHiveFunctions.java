package io.hivesql.sql.parser;

import org.testng.annotations.Test;

public class CreateHiveFunctions extends SQLTester {

    @Test
    public void testCreateFunction()
    {
        String sql = "CREATE FUNCTION addfunc AS 'com.example.hiveserver2.udf.add'";
        runHiveSQL(sql);
    }

    @Test
    public void testCreateTemporaryFunction()
    {
        String sql = "CREATE TEMPORARY FUNCTION addfunc AS 'com.example.hiveserver2.udf.add'";
        runHiveSQL(sql);
    }
}
