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
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestBigoIsAVFeature {
    @Test
    private void testIsAvFeature() {
        boolean res1 = BigoIsAVFeature.isAvFeature(1, 491);
        boolean res2 = BigoIsAVFeature.isAvFeature(2, 491);
        boolean res3 = BigoIsAVFeature.isAvFeature(3, 491);
        boolean res4 = BigoIsAVFeature.isAvFeature(4, 491);
        boolean res5 = BigoIsAVFeature.isAvFeature(1, 235);
        boolean res6 = BigoIsAVFeature.isAvFeature(2, 235);
        boolean res7 = BigoIsAVFeature.isAvFeature(3, 235);
        boolean res8 = BigoIsAVFeature.isAvFeature(4, 235);
        boolean res9 = BigoIsAVFeature.isAvFeature(1, 246251);
        boolean res10 = BigoIsAVFeature.isAvFeature(2, 246251);
        boolean res11 = BigoIsAVFeature.isAvFeature(3, 246251);
        boolean res12 = BigoIsAVFeature.isAvFeature(4, 246251);

        assertTrue(res1);
        assertFalse(res2);
        assertTrue(res3);
        assertFalse(res4);
        assertTrue(res5);
        assertFalse(res6);
        assertTrue(res7);
        assertFalse(res8);
        assertTrue(res9);
        assertFalse(res10);
        assertTrue(res11);
        assertFalse(res12);
    }
}
