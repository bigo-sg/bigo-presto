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
package io.prestosql.plugin.bigo;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.prestosql.plugin.bigo.udf.ArrayContainsFunction;
import io.prestosql.plugin.bigo.udf.ArraySizeFunction;
import io.prestosql.plugin.bigo.udf.AsciiFunction;
import io.prestosql.plugin.bigo.udf.BigoConditionalFunctions;
import io.prestosql.plugin.bigo.udf.BigoDateTimeFunctions;
import io.prestosql.plugin.bigo.udf.BigoDecodeUrl;
import io.prestosql.plugin.bigo.udf.BigoIndigoUidHash;
import io.prestosql.plugin.bigo.udf.BigoIndigoUidHashMod;
import io.prestosql.plugin.bigo.udf.BigoIsAVFeature;
import io.prestosql.plugin.bigo.udf.BigoIsAVTest;
import io.prestosql.plugin.bigo.udf.BigoStringFunctions;
import io.prestosql.plugin.bigo.udf.BigoTypeConversionFunctions;
import io.prestosql.plugin.bigo.udf.BinaryFunction;
import io.prestosql.plugin.bigo.udf.ConcatWsFunction;
import io.prestosql.plugin.bigo.udf.DecodeFunction;
import io.prestosql.plugin.bigo.udf.EncodeFunction;
import io.prestosql.plugin.bigo.udf.HexFunction;
import io.prestosql.plugin.bigo.udf.MD5Function;
import io.prestosql.plugin.bigo.udf.MapSizeFunction;
import io.prestosql.plugin.bigo.udf.PosModFunction;
import io.prestosql.plugin.bigo.udf.Sha2Function;
import io.prestosql.plugin.bigo.udf.ShaFunction;
import io.prestosql.plugin.bigo.udf.ShiftLeftFunction;
import io.prestosql.plugin.bigo.udf.ShiftRightFunction;
import io.prestosql.plugin.bigo.udf.ShiftRightUnsignedFunction;
import io.prestosql.plugin.bigo.udf.UnBase64;
import io.prestosql.plugin.bigo.udf.UnHexFunction;
import io.prestosql.spi.Plugin;
import io.prestosql.spi.eventlistener.EventListenerFactory;

import java.util.Set;

/**
 * @author tangyun@bigo.sg
 * @date 7/1/19 8:43 PM
 */
public class BigoEventListenerPlugin
        implements Plugin
{
    @Override
    public Iterable<EventListenerFactory> getEventListenerFactories()
    {
        return ImmutableList.of(new BigoEventListenerFactory());
    }

    @Override
    public Set<Class<?>> getFunctions()
    {
        return ImmutableSet.<Class<?>>builder()
                .add(BigoDateTimeFunctions.class)
                .add(BigoStringFunctions.class)
                .add(BigoConditionalFunctions.class)
                .add(HexFunction.class)
                .add(UnHexFunction.class)
                .add(PosModFunction.class)
                .add(ShiftRightFunction.class)
                .add(ShiftRightUnsignedFunction.class)
                .add(ShiftLeftFunction.class)
                .add(AsciiFunction.class)
                .add(ArrayContainsFunction.class)
                .add(BigoTypeConversionFunctions.class)
                .add(ArraySizeFunction.class)
                .add(MapSizeFunction.class)
                .add(BigoIndigoUidHash.class)
                .add(BigoIndigoUidHashMod.class)
                .add(BigoIsAVTest.class)
                .add(BigoDecodeUrl.class)
                .add(MD5Function.class)
                .add(ShaFunction.class)
                .add(Sha2Function.class)
                .add(DecodeFunction.class)
                .add(EncodeFunction.class)
                .add(BinaryFunction.class)
                .add(ConcatWsFunction.class)
                .add(UnBase64.class)
                .add(BigoIsAVFeature.class)
                .build();
    }
}
