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
package io.prestosql.sql.parser.hive;

import io.prestosql.sql.tree.AstVisitor;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.LikePredicate;
import io.prestosql.sql.tree.NodeLocation;

import java.util.Optional;

public class RlikePredicate
        extends LikePredicate
{

    public RlikePredicate(Expression value, Expression pattern, Expression escape)
    {
        this(Optional.empty(), value, pattern, Optional.of(escape));
    }

    public RlikePredicate(NodeLocation location, Expression value, Expression pattern, Optional<Expression> escape)
    {
        this(Optional.of(location), value, pattern, escape);
    }

    public RlikePredicate(Expression value, Expression pattern, Optional<Expression> escape)
    {
        this(Optional.empty(), value, pattern, escape);
    }

    private RlikePredicate(Optional<NodeLocation> location, Expression value, Expression pattern, Optional<Expression> escape)
    {
        super(location, value, pattern, escape);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitRlikePredicate(this, context);
    }

}
