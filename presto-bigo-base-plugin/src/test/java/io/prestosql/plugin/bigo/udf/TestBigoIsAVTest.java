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

import org.testng.annotations.Test;

import static io.airlift.slice.Slices.utf8Slice;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestBigoIsAVTest {
    @Test
    private void testIsAvTest() {
        boolean res1 = BigoIsAVTest.isAvTest(155, utf8Slice("YAAAAAgEwEWwAAgAAGQ0KIOjCAEUAECAeQ=="), 0);
        boolean res2 = BigoIsAVTest.isAvTest(155, null, 0);
        boolean res3 = BigoIsAVTest.isAvTest(155, utf8Slice("bRbWUYgEQkeAQAjqIQQ0CIGnKwEWHVr4dQ=="), 0);
        boolean res4 = BigoIsAVTest.isAvTest(155, utf8Slice("MKvFt4AMRk9AAAhgKHQ0KoOzKhEUAEiAfA=="), 0);
        boolean res5 = BigoIsAVTest.isAvTest(155, utf8Slice("IAAAABAJREUGQAgAICQkKIPzChEUAEhAeA=="), 0);
        boolean res6 = BigoIsAVTest.isAvTest(155, utf8Slice("bz73pQgMAk9QQAQgIUQkCoH3IxEUGVoQfQ=="), 0);

        assertFalse(res1);
        assertFalse(res2);
        assertFalse(res3);
        assertTrue(res4);
        assertTrue(res5);
        assertTrue(res6);
    }
}