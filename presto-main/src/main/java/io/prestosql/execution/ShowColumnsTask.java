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
package io.prestosql.execution;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import io.prestosql.Session;
import io.prestosql.client.ClientStandardTypes;
import io.prestosql.client.ClientTypeSignature;
import io.prestosql.client.ClientTypeSignatureParameter;
import io.prestosql.client.Column;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.MetadataListing;
import io.prestosql.metadata.MetadataUtil;
import io.prestosql.metadata.QualifiedTablePrefix;
import io.prestosql.security.AccessControl;
import io.prestosql.spi.connector.CatalogSchemaName;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.type.VarcharType;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.Identifier;
import io.prestosql.sql.tree.QualifiedName;
import io.prestosql.sql.tree.ShowColumns;
import io.prestosql.transaction.TransactionManager;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.util.concurrent.Futures.immediateFuture;
import static io.prestosql.spi.StandardErrorCode.CATALOG_NOT_FOUND;
import static io.prestosql.spi.StandardErrorCode.TABLE_NOT_FOUND;
import static io.prestosql.spi.type.VarcharType.createUnboundedVarcharType;
import static io.prestosql.sql.analyzer.SemanticExceptions.semanticException;

public class ShowColumnsTask
        extends BaseShowTask<ShowColumns>
{
    private static final List<Column> COLUMNS;

    static {
        COLUMNS = new ArrayList<>();

        VarcharType varcharType = createUnboundedVarcharType();
        ClientTypeSignatureParameter parameter = ClientTypeSignatureParameter.ofLong(Long.MAX_VALUE);
        ClientTypeSignature varcharSign = new ClientTypeSignature(ClientStandardTypes.VARCHAR, ImmutableList.of(parameter));

        Column column = new Column("Column", varcharType.getDisplayName(), varcharSign);
        COLUMNS.add(column);

        Column type = new Column("Type", varcharType.getDisplayName(), varcharSign);
        COLUMNS.add(type);

        Column extra = new Column("Extra", varcharType.getDisplayName(), varcharSign);
        COLUMNS.add(extra);

        Column comment = new Column("Comment", varcharType.getDisplayName(), varcharSign);
        COLUMNS.add(comment);
    }

    @Override
    public String getName()
    {
        return "SHOW COLUMNS";
    }

    @Override
    public String explain(ShowColumns statement, List<Expression> parameters)
    {
        return "SHOW COLUMNS " + statement.getTable();
    }

    @Override
    public ListenableFuture<?> execute(ShowColumns statement, TransactionManager transactionManager, Metadata metadata, AccessControl accessControl, QueryStateMachine stateMachine, List<Expression> parameters)
    {
        Session session = stateMachine.getSession();

        Optional<QualifiedName> schema = Optional.empty();
        if (statement.getTable().getOriginalParts().size() == 2) {
            Identifier schemaName = statement.getTable().getOriginalParts().get(0);
            schema = Optional.of(QualifiedName.of(schemaName.getValue()));
        }
        CatalogSchemaName catalogSchemaName = MetadataUtil.createCatalogSchemaName(session, statement, schema);

        if (!metadata.getCatalogHandle(session, catalogSchemaName.getCatalogName()).isPresent()) {
            throw semanticException(CATALOG_NOT_FOUND, statement, "Catalog '%s' does not exist", session.getCatalog().get());
        }

        QualifiedTablePrefix prefix = new QualifiedTablePrefix(catalogSchemaName.getCatalogName(),
                Optional.ofNullable(catalogSchemaName.getSchemaName()),
                Optional.of(statement.getTable().getSuffix()));

        Map<SchemaTableName, List<ColumnMetadata>> schemaTableNameListMap = MetadataListing.listTableColumns(session, metadata, accessControl, prefix);

        if (schemaTableNameListMap.isEmpty()) {
            throw semanticException(TABLE_NOT_FOUND, statement, "Table '%s' does not exist", statement.getTable().getSuffix());
        }

        List<List<Object>> ret = new ArrayList<>();

        for (List<ColumnMetadata> columnMetadataList : schemaTableNameListMap.values()) {
            for (ColumnMetadata columnMetadata : columnMetadataList) {
                List<Object> row = new ArrayList<>();

                row.add(columnMetadata.getName());
                row.add(columnMetadata.getType().getDisplayName());
                if (columnMetadata.getExtraInfo() == null) {
                    row.add("");
                } else {
                    row.add(columnMetadata.getExtraInfo());
                }
                if (columnMetadata.getComment() == null) {
                    row.add("");
                } else {
                    row.add(columnMetadata.getComment());
                }

                ret.add(row);
            }

        }

        lf = immediateFuture(ret);
        return lf;
    }

    @Override
    public List<Column> getColumns() {
        return COLUMNS;
    }
}
