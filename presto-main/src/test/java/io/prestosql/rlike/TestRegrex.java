package io.prestosql.rlike;

import org.testng.annotations.Test;

import java.util.regex.Pattern;

/**
 * @author tangyun@bigo.sg
 * @date 8/5/19 3:14 PM
 */
public class TestRegrex {

    @Test
    public void test01() {
        String content = "I am noob " +
                "from runoob.com.";
        String pattern = ".*runoob.*";

        boolean isMatch = Pattern.matches(pattern, content);
        System.out.println("字符串中是否包含了 'runoob' 子字符串? " + isMatch);
    }
}
