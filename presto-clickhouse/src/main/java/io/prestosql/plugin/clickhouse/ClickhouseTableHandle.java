package io.prestosql.plugin.clickhouse;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Joiner;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.predicate.TupleDomain;

import javax.annotation.Nullable;
import java.util.Objects;
import java.util.OptionalLong;

import static java.util.Objects.requireNonNull;

public class ClickhouseTableHandle implements ConnectorTableHandle
{
    private final SchemaTableName schemaTableName;

    // catalog, schema and table names are reported by the remote database
    private final String catalogName;
    private final String schemaName;
    private final String tableName;
    private final TupleDomain<ColumnHandle> constraint;
    private final OptionalLong limit;

    public ClickhouseTableHandle(SchemaTableName schemaTableName, @Nullable String catalogName, @Nullable String schemaName, String tableName)
    {
        this(schemaTableName, catalogName, schemaName, tableName, TupleDomain.all(), OptionalLong.empty());
    }

    @JsonCreator
    public ClickhouseTableHandle(
            @JsonProperty("schemaTableName") SchemaTableName schemaTableName,
            @JsonProperty("catalogName") @Nullable String catalogName,
            @JsonProperty("schemaName") @Nullable String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("constraint") TupleDomain<ColumnHandle> constraint,
            @JsonProperty("limit") OptionalLong limit)
    {
        this.schemaTableName = requireNonNull(schemaTableName, "schemaTableName is null");
        this.catalogName = catalogName;
        this.schemaName = schemaName;
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.constraint = requireNonNull(constraint, "constraint is null");
        this.limit = requireNonNull(limit, "limit is null");
    }

    @JsonProperty
    public SchemaTableName getSchemaTableName()
    {
        return schemaTableName;
    }

    @JsonProperty
    @Nullable
    public String getCatalogName()
    {
        return catalogName;
    }

    @JsonProperty
    @Nullable
    public String getSchemaName()
    {
        return schemaName;
    }

    @JsonProperty
    public String getTableName()
    {
        return tableName;
    }

    @JsonProperty
    public TupleDomain<ColumnHandle> getConstraint()
    {
        return constraint;
    }

    @JsonProperty
    public OptionalLong getLimit()
    {
        return limit;
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
        ClickhouseTableHandle o = (ClickhouseTableHandle) obj;
        return Objects.equals(this.schemaTableName, o.schemaTableName);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(schemaTableName);
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder();
        builder.append(schemaTableName).append(" ");
        Joiner.on(".").skipNulls().appendTo(builder, catalogName, schemaName, tableName);
        limit.ifPresent(value -> builder.append(" limit=").append(value));
        return builder.toString();
    }
}