package io;

import org.testng.annotations.Test;

import java.util.Arrays;

public class ArrayTests {

    @Test
    public void test01() {

        String[] s = new String[]{
                "11111", "adsdsadas", "adsasdas"
        };
        String[] m = Arrays.copyOfRange(s, 1, s.length);
        System.out.println(m);
    }
}
