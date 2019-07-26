package io.prestosql.sql.parser.hive;

import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.Identifier;
import io.prestosql.sql.tree.Node;
import io.prestosql.sql.tree.NodeLocation;

import java.util.List;
import java.util.Optional;

public class StarExpression extends Expression {
    private Optional<Identifier> identifier;

    public StarExpression(NodeLocation location, Optional<Identifier> identifier) {
        super(Optional.of(location));
        this.identifier = identifier;
    }

    public Optional<Identifier> getIdentifier() {
        return identifier;
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
