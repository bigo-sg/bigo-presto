package io.hivesql.sql.parser;

import io.prestosql.sql.parser.ParsingException;
import org.testng.annotations.Test;

public class UnsupportedSQLs extends SQLTester {

    @Test(expectedExceptions = ParsingException.class)
    public void sortByShouldThrowException()
    {
        String sql = "SELECT a from b sort by c";
        runHiveSQL(sql);
    }

    @Test(expectedExceptions = ParsingException.class)
    public void addFileShouldThrowException()
    {
        String sql = "add file /data/opt/hive/udf/ip_country.txt";
        runHiveSQL(sql);
    }

    @Test(expectedExceptions = ParsingException.class)
    public void addJarShouldThrowException()
    {
        String sql = "add jar /data/opt/hive/udf/ip_country.jar";
        runHiveSQL(sql);
    }
}
