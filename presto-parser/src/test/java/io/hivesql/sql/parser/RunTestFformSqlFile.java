package io.hivesql.sql.parser;

import io.utils.FileUtils;
import org.testng.annotations.Test;

import java.util.List;
import java.util.function.Consumer;

public class RunTestFformSqlFile extends SQLTester {

    public void readAndTestFile(String fileName)
    {
        byte[] data = FileUtils.getFileAsBytes(fileName);
        String sqls = new String(data);
        String[] sqlArray = sqls.split(";");
        System.out.println("check sql file:" + fileName);
        checkASTNode(sqlArray[1], sqlArray[0]);
    }

    @Test
    public void test01() {
        String rootPath = this.getClass().getResource("../../../../").getFile();
        System.out.println(rootPath);
        readAndTestFile(rootPath +  "hive/parser/r1.sql");
    }

    @Test
    public void testByScanDir() {
        String rootPath = this.getClass().getResource("../../../../hive/parser/pass").getFile();
        List<String> sqlFiles = FileUtils.scanDirectory(rootPath);
        sqlFiles.stream().forEach(new Consumer<String>() {
            @Override
            public void accept(String s) {
                if (s.endsWith(".sql")) {
                    readAndTestFile(s);
                }
            }
        });
    }
}
