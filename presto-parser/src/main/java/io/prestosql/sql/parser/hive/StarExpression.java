package io.prestosql.sql.parser.hive;

import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.Node;
import io.prestosql.sql.tree.NodeLocation;

import java.util.List;
import java.util.Optional;

public class StarExpression extends Expression {
    public StarExpression(NodeLocation location) {
        super(Optional.of(location));
    }

    @Override
    public List<? extends Node> getChildren() {
        return null;
    }

    @Override
    public int hashCode() {
        return 0;
    }

    @Override
    public boolean equals(Object obj) {
        return true;
    }
}
