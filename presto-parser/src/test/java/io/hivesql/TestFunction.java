package io.hivesql;

import io.hivesql.sql.parser.SqlBaseParser;
import org.antlr.v4.runtime.ParserRuleContext;
import org.testng.annotations.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.function.Function;

/**
 * @author tangyun@bigo.sg
 * @date 7/16/19 11:33 AM
 */
public class TestFunction {

    @Test
    public void test01() {
        Function<SqlBaseParser, ParserRuleContext> parseFunction1 = io.hivesql.sql.parser.SqlBaseParser::singleStatement;

        for (Field field: parseFunction1.getClass().getFields()) {
            System.out.println(field.toString());
        }
        for (Method field: parseFunction1.getClass().getMethods()) {
            System.out.println(field.toString());
        }

    }
}
