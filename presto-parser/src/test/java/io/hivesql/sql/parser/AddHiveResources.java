package io.hivesql.sql.parser;

import org.testng.annotations.Test;

public class AddHiveResources extends SQLTester {

    @Test
    public void testAddFile()
    {
        String sql = "add file /data/opt/hive/udf/ip_country.txt";
        runHiveSQL(sql);
    }

    @Test
    public void testAddJar()
    {
        String sql = "add jar /data/opt/hive/udf/ip_country.jar";
        runHiveSQL(sql);
    }
}