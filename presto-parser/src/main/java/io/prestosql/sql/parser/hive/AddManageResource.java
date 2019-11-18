package io.prestosql.sql.parser.hive;

import io.prestosql.sql.tree.Node;
import io.prestosql.sql.tree.NodeLocation;
import io.prestosql.sql.tree.Statement;

import java.util.List;
import java.util.Optional;

public class AddManageResource extends Statement {

    protected AddManageResource(Optional<NodeLocation> location) {
        super(location);
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
        return false;
    }

    @Override
    public String toString() {
        return null;
    }
}