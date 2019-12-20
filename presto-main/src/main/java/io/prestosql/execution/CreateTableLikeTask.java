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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;
import io.prestosql.Session;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.MetadataUtil;
import io.prestosql.metadata.TableMetadata;
import io.prestosql.metadata.QualifiedObjectName;
import io.prestosql.metadata.TableHandle;
import io.prestosql.security.AccessControl;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.sql.parser.hive.CreateTableLike;
import io.prestosql.sql.tree.Expression;
import io.prestosql.transaction.TransactionManager;

import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

import static com.google.common.util.concurrent.Futures.immediateFuture;
import static io.prestosql.metadata.MetadataUtil.createQualifiedObjectName;
import static io.prestosql.spi.StandardErrorCode.*;
import static io.prestosql.sql.analyzer.SemanticExceptions.semanticException;

public class CreateTableLikeTask
        implements DataDefinitionTask<CreateTableLike>
{
    @Override
    public String getName()
    {
        return "CREATE TABLE LIKE";
    }

    @Override
    public String explain(CreateTableLike statement, List<Expression> parameters)
    {
        return "CREATE TABLE "+ statement.getName() +" LIKE " + statement.getName();
    }

    @Override
    public ListenableFuture<?> execute(CreateTableLike statement, TransactionManager transactionManager, Metadata metadata, AccessControl accessControl, QueryStateMachine stateMachine, List<Expression> parameters)
    {
        return internalExecute(statement, metadata, accessControl, stateMachine.getSession(), parameters);
    }

    @VisibleForTesting
    public ListenableFuture<?> internalExecute(CreateTableLike statement, Metadata metadata, AccessControl accessControl, Session session, List<Expression> parameters)
    {
        QualifiedObjectName qualifiedObjectName = MetadataUtil.createQualifiedObjectName(session, statement, statement.getSource());
        Optional<TableHandle> tableHandle = metadata.getTableHandle(session, qualifiedObjectName);
        if (!tableHandle.isPresent()) {
            throw semanticException(TABLE_NOT_FOUND, statement, "source table %s not exists", qualifiedObjectName.toString());
        }
        TableMetadata tableMetadata = metadata.getTableMetadata(session, tableHandle.get());
        QualifiedObjectName tableName = createQualifiedObjectName(session, statement, statement.getName());
        tableHandle = metadata.getTableHandle(session, tableName);
        if (tableHandle.isPresent()) {
            if (!statement.isNotExists()) {
                throw semanticException(TABLE_ALREADY_EXISTS, statement, "Table '%s' already exists", tableName);
            }
            return immediateFuture(null);
        }

        accessControl.checkCanCreateTable(session.toSecurityContext(), tableName);
        ImmutableMap.Builder properties = ImmutableMap.builder().putAll(tableMetadata.getMetadata().getProperties());
        if (!tableMetadata.getMetadata().getProperties().containsKey("sorted_by")) {
            properties.put("sorted_by", ImmutableList.of());
        }
        if (!tableMetadata.getMetadata().getProperties().containsKey("orc_bloom_filter_fpp")) {
            properties.put("orc_bloom_filter_fpp", 0.05);
        }
        if (!tableMetadata.getMetadata().getProperties().containsKey("partitioned_by")) {
            properties.put("partitioned_by", ImmutableList.of());
        }
        if (!tableMetadata.getMetadata().getProperties().containsKey("bucket_by")) {
            properties.put("bucketed_by", ImmutableList.of());
        }
        if (!tableMetadata.getMetadata().getProperties().containsKey("orc_bloom_filter_columns")) {
            properties.put("orc_bloom_filter_columns", ImmutableList.of());
        }
        if (!tableMetadata.getMetadata().getProperties().containsKey("bucket_count")) {
            properties.put("bucket_count", 0);
        }
        ImmutableList.Builder columnMetadatas = ImmutableList.builder();
        tableMetadata.getColumns().stream().forEach(new Consumer<ColumnMetadata>() {
            @Override
            public void accept(ColumnMetadata columnMetadata) {
                if (!columnMetadata.getName().startsWith("$")) {
                    columnMetadatas.add(columnMetadata);
                }
            }
        });
        ConnectorTableMetadata newTableMetadata = new ConnectorTableMetadata(
                tableName.asSchemaTableName(),
                columnMetadatas.build(),
                properties.build(),
                tableMetadata.getMetadata().getComment()
        );
        try {
            metadata.createTable(session, tableName.getCatalogName(), newTableMetadata, statement.isNotExists());
        }
        catch (PrestoException e) {
            // connectors are not required to handle the ignoreExisting flag
            if (!e.getErrorCode().equals(ALREADY_EXISTS.toErrorCode()) || !statement.isNotExists()) {
                throw e;
            }
        }
        return immediateFuture(null);
    }
}
