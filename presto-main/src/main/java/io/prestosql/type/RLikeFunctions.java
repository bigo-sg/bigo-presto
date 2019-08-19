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
package io.prestosql.type;

import io.airlift.slice.Slice;
import io.prestosql.spi.function.*;
import io.prestosql.spi.type.StandardTypes;

import java.util.regex.Pattern;

import static io.prestosql.spi.type.Chars.padSpaces;

public final class RLikeFunctions
{

    private RLikeFunctions() {}

    // TODO: this should not be callable from SQL
    @ScalarFunction(value = "rlike", hidden = true)
    @LiteralParameters("x")
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean likeVarchar(@SqlType("varchar(x)") Slice value, @SqlType("varchar(x)") Slice pattern)
    {
        Pattern p = Pattern.compile(new String(pattern.getBytes()));
        return p.matcher(new String(value.getBytes())).find();
    }

    @ScalarOperator(OperatorType.CAST)
    @LiteralParameters("x")
    @SqlType(RLikePatternType.NAME)
    public static Pattern castVarCharToRLikePattern(@SqlType("varchar(x)") Slice pattern)
    {
        return Pattern.compile(new String(pattern.getBytes()));
    }


    @ScalarOperator(OperatorType.CAST)
    @LiteralParameters("x")
    @SqlType(RLikePatternType.NAME)
    public static Pattern castCharToLikePattern(@LiteralParameter("x") Long charLength, @SqlType("char(x)") Slice pattern)
    {
        return Pattern.compile(new String(padSpaces(pattern, charLength.intValue()).getBytes()));
    }

    // TODO: this should not be callable from SQL
    @ScalarFunction(value = "rlike", hidden = true)
    @LiteralParameters("x")
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean likeVarchar(@SqlType("varchar(x)") Slice value, @SqlType(RLikePatternType.NAME) Pattern pattern)
    {
        return pattern.matcher(new String(value.getBytes())).find();
    }

    @ScalarFunction(value = "rlike", hidden = true)
    @LiteralParameters("x")
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean rLikeChar(@LiteralParameter("x") Long x, @SqlType("char(x)") Slice value, @SqlType(LikePatternType.NAME) Pattern pattern)
    {
        return likeVarchar(padSpaces(value, x.intValue()), pattern);
    }
}