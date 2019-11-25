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
    public void listResourceThrowException()
    {
        String sql = "LIST FILES";
        runHiveSQL(sql);
    }

    @Test(expectedExceptions = ParsingException.class)
    public void distinctOnThrowException()
    {
        String sql = "SELECT distinct on a from tb1";

        runHiveSQL(sql);
    }

    @Test(expectedExceptions = ParsingException.class)
    public void missingSelectStatementShouldThrowException()
    {
        String sql = "from tb1 where a > 10";

        runHiveSQL(sql);
    }

    @Test(expectedExceptions = ParsingException.class)
    public void loadDataShouldThrowException()
    {
        String sql = "load data inpath '/directory-path/file.csv' into tbl";

        runHiveSQL(sql);
    }
}
