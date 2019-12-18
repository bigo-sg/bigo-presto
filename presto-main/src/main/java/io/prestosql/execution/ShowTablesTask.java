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
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.type.VarcharType;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.ShowTables;
import io.prestosql.transaction.TransactionManager;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.util.concurrent.Futures.immediateFuture;
import static io.prestosql.spi.StandardErrorCode.CATALOG_NOT_FOUND;
import static io.prestosql.spi.StandardErrorCode.SCHEMA_NOT_FOUND;
import static io.prestosql.spi.type.VarcharType.createUnboundedVarcharType;
import static io.prestosql.sql.analyzer.SemanticExceptions.semanticException;

public class ShowTablesTask
        extends BaseShowTask<ShowTables>
{
    private static final List<Column> COLUMNS;

    static {
        COLUMNS = new ArrayList<>();

        VarcharType varcharType = createUnboundedVarcharType();
        ClientTypeSignatureParameter parameter = ClientTypeSignatureParameter.ofLong(Long.MAX_VALUE);
        ClientTypeSignature varcharSign = new ClientTypeSignature(ClientStandardTypes.VARCHAR, ImmutableList.of(parameter));

        Column column = new Column("Table", varcharType.getDisplayName(), varcharSign);
        COLUMNS.add(column);
    }

    @Override
    public String getName()
    {
        return "SHOW TABLES";
    }

    @Override
    public String explain(ShowTables statement, List<Expression> parameters)
    {
        return "SHOW TABLES " + statement.getSchema();
    }

    @Override
    public ListenableFuture<?> execute(ShowTables statement, TransactionManager transactionManager, Metadata metadata, AccessControl accessControl, QueryStateMachine stateMachine, List<Expression> parameters)
    {
        Session session = stateMachine.getSession();

        CatalogSchemaName catalogSchemaName = MetadataUtil.createCatalogSchemaName(session, statement, statement.getSchema());

        if (!metadata.getCatalogHandle(session, catalogSchemaName.getCatalogName()).isPresent()) {
            throw semanticException(CATALOG_NOT_FOUND, statement, "Catalog '%s' does not exist", session.getCatalog().get());
        }

        QualifiedTablePrefix prefix = new QualifiedTablePrefix(catalogSchemaName.getCatalogName(),
                Optional.ofNullable(catalogSchemaName.getSchemaName()),
                statement.getLikePattern());

        Set<SchemaTableName> tables = MetadataListing.listTables(session, metadata, accessControl, prefix);

        if (tables.isEmpty()) {
            // TODO: this will throw exception if schema is empty.
            throw semanticException(SCHEMA_NOT_FOUND, statement, "Schema '%s' does not exist", catalogSchemaName.getSchemaName());
        }

        List<List<Object>> ret = new ArrayList<>();

        for (SchemaTableName table : tables) {
            List<Object> row = new ArrayList<>();

            row.add(table.getTableName());

            ret.add(row);
        }

        lf = immediateFuture(ret);
        return lf;
    }

    @Override
    public List<Column> getColumns() {
        return COLUMNS;
    }
}
