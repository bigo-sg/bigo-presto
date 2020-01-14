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
import com.google.common.util.concurrent.ListenableFuture;
import io.prestosql.Session;
import io.prestosql.metadata.QualifiedObjectName;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.MetadataUtil;
import io.prestosql.metadata.TableHandle;
import io.prestosql.metadata.TableMetadata;
import io.prestosql.security.AccessControl;
import io.prestosql.spi.PrestoException;
import io.prestosql.sql.parser.hive.LoadData;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.SingleColumn;
import io.prestosql.transaction.TransactionManager;
import javafx.util.Pair;

import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Optional;

import static com.google.common.util.concurrent.Futures.immediateFuture;
import static io.prestosql.spi.StandardErrorCode.INVALID_SPATIAL_PARTITIONING;
import static io.prestosql.spi.StandardErrorCode.TABLE_NOT_FOUND;
import static io.prestosql.sql.analyzer.SemanticExceptions.semanticException;

/**
 * @author tangyun@bigo.sg
 * @date 1/4/20 8:14 PM
 */

public class LoadDataTask
        implements DataDefinitionTask<LoadData>
{
    @Override
    public String getName()
    {
        return "LOAD DATA";
    }

    @Override
    public String explain(LoadData statement, List<Expression> parameters)
    {
        return "LOAD DATA";
    }

    @Override
    public ListenableFuture<?> execute(LoadData statement, TransactionManager transactionManager, Metadata metadata, AccessControl accessControl, QueryStateMachine stateMachine, List<Expression> parameters)
    {
        return internalExecute(statement, metadata, accessControl, stateMachine.getSession(), parameters);
    }

    @VisibleForTesting
    public ListenableFuture<?> internalExecute(LoadData statement, Metadata metadata, AccessControl accessControl, Session session, List<Expression> parameters)
    {
        QualifiedObjectName qualifiedObjectName = MetadataUtil.createQualifiedObjectName(session, statement, statement.getName());
        Optional<TableHandle> tableHandle = metadata.getTableHandle(session, qualifiedObjectName);
        if (!tableHandle.isPresent()) {
            throw semanticException(TABLE_NOT_FOUND, statement, "target table %s not exists", qualifiedObjectName.toString());
        }
        TableMetadata tableMetadata = metadata.getTableMetadata(session, tableHandle.get());
        Object o = tableMetadata.getMetadata().getProperties().get("partitioned_by");
        List<Pair<String, String>> partitions = null;
        if (o != null) {
            partitions = getPartitions(statement.getPartitions(),
                    (ArrayList<String>)o, statement);

        }
        accessControl.checkCanInsertIntoTable(session.toSecurityContext(), qualifiedObjectName);
        if (statement.isOverwrite()) {
            accessControl.checkCanDeleteFromTable(session.toSecurityContext(), qualifiedObjectName);
        }

        try {
            metadata.loadData(session, tableHandle.get(), qualifiedObjectName, statement.getPath(), statement.isOverwrite(), getPartitionEnd(partitions));
        }
        catch (PrestoException e) {
            // connectors are not required to handle the ignoreExisting flag
            if (!e.getErrorCode().equals(TABLE_NOT_FOUND.toErrorCode())) {
                throw e;
            }
        }
        return immediateFuture(null);
    }

    public static List<Pair<String, String>> getPartitions(List<SingleColumn> loadPartitions,
                                                     List<String> tablePartitions, LoadData statement) {
        if (loadPartitions.size() != tablePartitions.size()) {
            throw semanticException(INVALID_SPATIAL_PARTITIONING, statement, "invalid partitions");
        }
        Map<String, String> partitionKeyAndValue = new HashMap<>();
        loadPartitions.stream().forEach(singleColumn -> {
            partitionKeyAndValue.put(singleColumn.getAlias().get().toString()
                            .replace("`", "")
                            .replaceAll("\"", "")
                            .replaceAll("'", ""),
                    singleColumn.getExpression().toString()
                            .replace("'", "")
                            .replace("\"", ""));
        });
        List<Pair<String, String>> result = new ArrayList<>();
        for (String partition : tablePartitions) {
            String value = partitionKeyAndValue.get(partition);
            if (value == null) {
                throw semanticException(INVALID_SPATIAL_PARTITIONING, statement, "need partition " + partition);
            }
            result.add(new Pair<>(partition, value));
        }
        return result;
    }

    public static String getPartitionEnd(List<Pair<String, String>> partitions) {
        if (partitions == null) {
            return "";
        }
        StringBuilder stringBuilder = new StringBuilder();
        if (partitions.size() > 0) {
            stringBuilder.append("/");
            partitions.stream().forEach(pair -> {
                stringBuilder.append(pair.getKey());
                stringBuilder.append("=");
                stringBuilder.append(pair.getValue());
                stringBuilder.append("/");
            });
        }
        return stringBuilder.toString();
    }
}
