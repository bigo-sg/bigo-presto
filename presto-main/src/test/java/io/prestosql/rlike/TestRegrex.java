package io.prestosql.rlike;

import org.testng.annotations.Test;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author tangyun@bigo.sg
 * @date 8/5/19 3:14 PM
 */
public class TestRegrex {

    @Test
    public void test01() {

        final String REGEX = "foo";
        final String INPUT = "fooooooooooooooooo";
        Pattern p;
        Matcher matcher;
        p = Pattern.compile(REGEX);
        matcher = p.matcher(INPUT);

        System.out.println("Current REGEX is: "+REGEX);
        System.out.println("Current INPUT is: "+INPUT);

        System.out.println("lookingAt(): "+matcher.lookingAt());
        System.out.println("matches(): "+matcher.matches());
        System.out.println(p.matcher("000fooooooooooooooooo").lookingAt());
        System.out.println(p.matcher("000fooooooooooooooooo").find());
     }
}
