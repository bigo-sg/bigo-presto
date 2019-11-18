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
import org.testng.annotations.Test;
import static org.testng.Assert.assertNull;

public class TestUnBase64 {
    @Test
    private void testUnBase64() {
        Slice slice1 = UnBase64.unBase64Varchar(null);
        Slice slice2 = UnBase64.unBase64Varbinary(null);

        assertNull(slice1);
        assertNull(slice2);
    }
}
