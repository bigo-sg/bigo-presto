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
import io.prestosql.spi.function.Description;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.SqlNullable;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.function.TypeParameter;
import io.prestosql.spi.type.StandardTypes;
import org.apache.commons.lang.StringUtils;

import static io.airlift.slice.Slices.utf8Slice;

public class SubstringIndexFunction {
    @Description("Returns the substring from string A before count occurrences of separator.")
    @ScalarFunction("substring_index")
    @SqlType(StandardTypes.VARCHAR)
    @TypeParameter("T")
    @SqlNullable
    public static Slice substringIndex(@SqlType(StandardTypes.VARCHAR) Slice slice,
                                       @SqlType(StandardTypes.VARCHAR) Slice separator,
                                       @SqlType("T") long count) {
        if (slice == null || slice.length() == 0 || separator == null || separator.length() == 0 || count == 0) {
            return null;
        }
        String str = slice.toStringUtf8();
        String sep = separator.toStringUtf8();
        int n = (int) count;

        if (!str.contains(sep)) {
            return slice;
        }
        String res;

        if (count > 0) {
            int index = StringUtils.ordinalIndexOf(str, sep, n);
            if (index != -1) {
                res = str.substring(0, index);
            }
            else {
                res = str;
            }
        }
        else {
            int index = StringUtils.lastOrdinalIndexOf(str, sep, -n);
            if (index != -1) {
                res = str.substring(index + 1);
            }
            else {
                res = str;
            }
        }

        return utf8Slice(res);
    }
}
