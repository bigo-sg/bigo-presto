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
import io.airlift.slice.Slices;
import io.prestosql.spi.function.Description;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.SqlNullable;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.function.TypeParameter;
import io.prestosql.spi.type.StandardTypes;

import java.util.Base64;

public class BigoIsAVTest {

    @Description("self build hive udf, isavtest")
    @ScalarFunction
    @SqlType(StandardTypes.BOOLEAN)
    @TypeParameter("T")
    public static boolean isAvTest(@SqlType("T") long id,
                                   @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice target,
                                   @SqlType("T") long abFirst) {
        if (target == null) {
            return false;
        }
        try {
            Slice slice = Slices.wrappedBuffer(Base64.getDecoder().decode(target.getBytes()));
            return isAvTestUnBase64(id, slice, abFirst);
        } catch (Exception e) {
            return false;
        }
    }

    @Description("self build hive udf, isavtest")
    @ScalarFunction("isavtest")
    @SqlType(StandardTypes.BOOLEAN)
    @TypeParameter("T")
    public static boolean isAvTestUnBase64(@SqlType("T") long id,
                                           @SqlNullable @SqlType(StandardTypes.VARBINARY) Slice target,
                                           @SqlType("T") long abFirst) {
        if (target == null) {
            return false;
        }

        byte[] abVector = target.getBytes();
        id -= abFirst;
        if (id >= 0) {
            int B = (int) (id / 8);
            int b = (int) (id % 8);
            if (B >= 0 && B < abVector.length) {
                return (abVector[abVector.length - 1 - B] & (1 << b)) != 0;
            }
        }
        return false;
    }
}