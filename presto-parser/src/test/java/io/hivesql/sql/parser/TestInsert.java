package io.hivesql.sql.parser;

import io.prestosql.sql.parser.ParsingException;
import org.testng.annotations.Test;

public class TestInsert extends SQLTester {

    @Test
    public void insertIntoSelect() {
        checkASTNodeFromFile("hive/parser/cases/insert-into-select-hive.sql");
    }

    @Test
    public void insertOverwriteSelect() {
        checkASTNodeFromFile("hive/parser/cases/insert-overwrite-select-presto.sql",
                "hive/parser/cases/insert-overwrite-select-hive.sql");
    }

    @Test
    public void insertOverwriteSelect1() {
        checkASTNodeFromFile("hive/parser/cases/insert-overwrite-select-presto.sql",
                "hive/parser/cases/insert-overwrite-select-hive1.sql");
    }

    @Test
    public void insertOverwriteSelect2() {
        checkASTNodeFromFile("hive/parser/cases/insert-overwrite-select-presto2.sql",
                "hive/parser/cases/insert-overwrite-select-hive2.sql");
    }

    @Test
    public void insertOverwriteSelect3() {
        checkASTNodeFromFile("hive/parser/cases/insert-overwrite-select-presto4.sql",
                "hive/parser/cases/insert-overwrite-select-hive4.sql");
    }

    @Test
    public void insertOverwriteSelectOnlineCase() {
        checkASTNodeFromFile("hive/parser/cases/insert-overwrite-select-presto3.sql",
                "hive/parser/cases/insert-overwrite-select-hive3.sql");
    }

    @Test(expectedExceptions = ParsingException.class)
    public void insertOverwriteSelect4() {
        runHiveSQLFromFile("hive/parser/cases/insert-overwrite-select-hive5.sql");
    }
}
