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
import io.prestosql.plugin.bigo.udf.*;
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
                .add(BigoIsAVFeature.class)
                .add(UnBase64.class)
                .add(SubstringIndexFunction.class)
                .add(IsNullFunction.class)
                .add(MiscFunctions.class)
                .build();
    }
}
