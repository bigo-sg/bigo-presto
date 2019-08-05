package io;

import io.prestosql.sql.parser.hive.HiveAstBuilder;
import org.testng.annotations.Test;

public class StringTests {

    @Test
    public void testo1() {
        System.out.println(HiveAstBuilder.tryUnquote("\"erwrwerwerw\""));
        System.out.println(HiveAstBuilder.tryUnquote("\'erwrwerwerw\'"));
        System.out.println(HiveAstBuilder.tryUnquote("`erwrwerwerw`"));

    }
}
