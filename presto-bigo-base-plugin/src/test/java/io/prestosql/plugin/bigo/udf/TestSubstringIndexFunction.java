/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.prestosql.plugin.bigo.udf;

import io.airlift.slice.Slice;
import org.locationtech.jts.util.Assert;
import org.testng.annotations.Test;

import static io.airlift.slice.Slices.utf8Slice;
import static org.testng.Assert.assertNull;

public class TestSubstringIndexFunction {
    @Test
    private void testSubstringIndex() {
        Slice slice1 = SubstringIndexFunction.substringIndex(null, utf8Slice("."), 1L);
        Slice slice2 = SubstringIndexFunction.substringIndex(utf8Slice("www.google.com"), null, 1L);
        Slice slice3 = SubstringIndexFunction.substringIndex(utf8Slice("www.google.com"), utf8Slice("."), 0);
        String str4 = SubstringIndexFunction.substringIndex(utf8Slice("www.google.com"), utf8Slice("."), 1L).toStringUtf8();
        String str5 = SubstringIndexFunction.substringIndex(utf8Slice("www.google.com"), utf8Slice("."), 2L).toStringUtf8();
        String str6 = SubstringIndexFunction.substringIndex(utf8Slice("www.google.com"), utf8Slice("."), 3L).toStringUtf8();
        String str7 = SubstringIndexFunction.substringIndex(utf8Slice("www.google.com"), utf8Slice("."), 10L).toStringUtf8();
        String str8 = SubstringIndexFunction.substringIndex(utf8Slice("www.google.com"), utf8Slice("."), -1L).toStringUtf8();
        String str9 = SubstringIndexFunction.substringIndex(utf8Slice("www.google.com"), utf8Slice("."), -2L).toStringUtf8();
        String str10 = SubstringIndexFunction.substringIndex(utf8Slice("www.google.com"), utf8Slice("."), -3L).toStringUtf8();
        String str11 = SubstringIndexFunction.substringIndex(utf8Slice("www.google.com"), utf8Slice("."), -10L).toStringUtf8();

        assertNull(slice1);
        assertNull(slice2);
        assertNull(slice3);
        Assert.equals(str4, "www");
        Assert.equals(str5, "www.google");
        Assert.equals(str6, "www.google.com");
        Assert.equals(str7, "www.google.com");
        Assert.equals(str8, "com");
        Assert.equals(str9, "google.com");
        Assert.equals(str10, "www.google.com");
        Assert.equals(str11, "www.google.com");

    }
}
