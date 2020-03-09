package io.prestosql.sql.parser.hive;


import io.prestosql.sql.tree.QualifiedName;
import io.prestosql.sql.tree.SingleColumn;
import io.prestosql.sql.tree.NodeLocation;
import io.prestosql.sql.tree.Statement;
import io.prestosql.sql.tree.AstVisitor;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * @author tangyun@bigo.sg
 * @date 1/3/20 8:14 PM
 */
public class LoadData
        extends Statement {
    private final QualifiedName name;
    private final String path;
    private final boolean overwrite;
    private final List<SingleColumn> partitions;

    public LoadData(Optional<NodeLocation> location, QualifiedName name,
                    String path, boolean overwrite, List<SingleColumn> partitions) {
        super(location);
        this.name = name;
        this.path = path;
        this.overwrite = overwrite;
        this.partitions = partitions;
    }

    public QualifiedName getName()
    {
        return name;
    }
    public String getPath() {
        return path;
    }
    public boolean isOverwrite() {
        return overwrite;
    }

    public List<SingleColumn> getPartitions() {
        return partitions;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitLoadData(this, context);
    }

    @Override
    public List<SingleColumn> getChildren()
    {
        return partitions;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, path, overwrite, partitions);
    }

    // for test
    public String getPartitionEnd() {
        StringBuilder stringBuilder = new StringBuilder();
        if (partitions.size() > 0) {
            stringBuilder.append("/");
            partitions.stream().forEach(singleColumn -> {
                stringBuilder.append(singleColumn.getAlias().get().getValue().replaceAll("'", ""));
                stringBuilder.append("=");
                stringBuilder.append(singleColumn.getExpression().toString().replaceAll("'", ""));
                stringBuilder.append("/");
            });
        }
        return stringBuilder.toString();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        LoadData o = (LoadData) obj;
        return Objects.equals(name, o.name) &&
                Objects.equals(path, o.path) &&
                Objects.equals(partitions, o.partitions) &&
                Objects.equals(overwrite, o.overwrite);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("name", name)
                .add("path", path)
                .add("overwrite", overwrite)
                .add("partitions", partitions)
                .toString();
    }
}
