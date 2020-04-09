package io.prestosql.sql.rewrite;

import io.airlift.log.Logger;
import io.prestosql.Session;
import io.prestosql.SystemSessionProperties;
import io.prestosql.execution.warnings.WarningCollector;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.QualifiedObjectName;
import io.prestosql.metadata.TableHandle;
import io.prestosql.security.AccessControl;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.VarcharType;
import io.prestosql.sql.analyzer.QueryExplainer;
import io.prestosql.sql.parser.SqlParser;
import io.prestosql.sql.tree.*;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class PartitionTypeRewrite implements StatementRewrite.Rewrite {

    private static final Logger log = Logger.get(PartitionTypeRewrite.class);

    private static final Set<String> columnNamesToBeCast = new HashSet<>();

    private static final String KEYWORD_DAY = "day";
    private static final String KEYWORD_HOUR = "hour";
    private static final String KEYWORD_EVENT_ID = "event_id";

    static {
        // add keywords that are needed to be cast here
        columnNamesToBeCast.add(KEYWORD_DAY);
        columnNamesToBeCast.add(KEYWORD_HOUR);
        columnNamesToBeCast.add(KEYWORD_EVENT_ID);
    }

    @Override
    public Statement rewrite(Session session, Metadata metadata, SqlParser parser, Optional<QueryExplainer> queryExplainer,
                             Statement node, List<Expression> parameters, Map<NodeRef<Parameter>, Expression> parameterLookup,
                             AccessControl accessControl, WarningCollector warningCollector) {
        Visitor visitor = new Visitor(metadata, parser, session, accessControl);
        Statement statement = (Statement) visitor.process(node);
        return statement;
    }

    private static class Visitor
            extends ReturnableTraversalVisitor {
        private final Metadata metadata;
        private final Session session;
        private final SqlParser sqlParser;
        private final AccessControl accessControl;

        public Visitor(Metadata metadata, SqlParser sqlParser, Session session, AccessControl accessControl) {
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");
            this.session = requireNonNull(session, "session is null");
            this.accessControl = requireNonNull(accessControl, "accessControl is null");
        }

        @Override
        protected Node visitQuerySpecification(QuerySpecification node, Void context) {
            if (!SystemSessionProperties.isEnableHiveSqlSynTax(session)) {
                return node;
            }

            try {
                if (node.getWhere().isPresent()) {
                    if (node.getWhere().get() instanceof LogicalBinaryExpression) {
                        accessBinaryExp(node, (LogicalBinaryExpression) node.getWhere().get());
                    } else if (node.getWhere().get() instanceof ComparisonExpression) {
                        rewriteComparisionExpression(node, (ComparisonExpression) node.getWhere().get());
                    }
                }
            } catch (Exception e) {
                log.warn(e.getMessage());
            }
            return node;
        }

        private Node accessBinaryExp(QuerySpecification node, LogicalBinaryExpression binExp)
                throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {
            if (binExp.getLeft() instanceof LogicalBinaryExpression) {
                accessBinaryExp(node, (LogicalBinaryExpression) binExp.getLeft());
            } else if (binExp.getLeft() instanceof ComparisonExpression) {
                rewriteComparisionExpression(node, (ComparisonExpression) binExp.getLeft());
            }

            if (binExp.getRight() instanceof LogicalBinaryExpression) {
                accessBinaryExp(node, (LogicalBinaryExpression) binExp.getRight());
            } else if (binExp.getRight() instanceof ComparisonExpression) {
                rewriteComparisionExpression(node, (ComparisonExpression) binExp.getRight());
            }
            return node;
        }

        private Node rewriteComparisionExpression(QuerySpecification node, ComparisonExpression comparisonExpression)
                throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {
            if (comparisonExpression.getOperator().getValue().equals("=")) {
                if (node.getFrom().isPresent()) {
                    Map<String, ColumnHandle> allColumnHandles = getColumnHandles(node.getFrom().get());
                    // deal with the left one within the expression
                    Type leftType = null;
                    String columnName = null;
                    if (comparisonExpression.getLeft() instanceof Identifier) {
                        Identifier left = (Identifier) comparisonExpression.getLeft();
                        columnName = left.getValue();
                        // judge whether the column name is one of the keywords or not
                        if (columnNamesToBeCast.contains(columnName) && allColumnHandles.containsKey(columnName)) {
                            ColumnHandle columnHandle = allColumnHandles.get(columnName);
                            boolean isHiveCatalog = isHiveCatalog(columnHandle);
                            if (isHiveCatalog) {
                                boolean partitionKey = isPartitionKey(columnHandle);
                                if (!partitionKey) {
                                    return node;
                                }
                                leftType = getColumnType(columnHandle);
                            }
                        }
                    } else {
                        return node;
                    }
                    if (null == leftType) {
                        return node;
                    }

                    // deal with the right one within the expression
                    if (leftType instanceof VarcharType && comparisonExpression.getRight() instanceof LongLiteral) {
                        LongLiteral right = (LongLiteral) comparisonExpression.getRight();
                        long columnValue = right.getValue();
                        StringLiteral rewroteLiteral;
                        if (columnName.equals(KEYWORD_HOUR) && columnValue < 10L) {
                            rewroteLiteral = new StringLiteral("0" + columnValue);
                        } else {
                            rewroteLiteral = new StringLiteral("" + columnValue);
                        }
                        comparisonExpression.setRight(rewroteLiteral);
                    } else {
                        return node;
                    }
                }
            }
            return node;
        }

        private Map<String, ColumnHandle> getColumnHandles(Relation relation) {
            if (relation instanceof Table) {
                return getColumnHandles((Table) relation);
            }
            if (relation instanceof AliasedRelation) {
                AliasedRelation aliasedRelation = (AliasedRelation) relation;
                if (aliasedRelation.getRelation() instanceof TableSubquery) {
                    TableSubquery tableSubquery = (TableSubquery) aliasedRelation.getRelation();
                    QueryBody queryBody = tableSubquery.getQuery().getQueryBody();
                    if (queryBody instanceof QuerySpecification) {
                        QuerySpecification querySpecification = (QuerySpecification) queryBody;
                        Relation nextRelation = querySpecification.getFrom().get();
                        if (nextRelation instanceof Table) {
                            return getColumnHandles((Table) nextRelation);
                        } else {
                            return getColumnHandles(nextRelation);
                        }
                    }
                } else if (aliasedRelation.getRelation() instanceof Table) {
                    return getColumnHandles((Table) aliasedRelation.getRelation());
                }
            }
            return new HashMap<>();
        }

        private Map<String, ColumnHandle> getColumnHandles(Table table) {
            QualifiedObjectName qualifiedTableName = new QualifiedObjectName(session.getCatalog().get(), session.getSchema().get(), table.getName().toString());
            TableHandle tableHandle = metadata.getTableHandle(session, qualifiedTableName)
                    .orElseThrow(() -> new IllegalArgumentException(format("Table %s does not exist", qualifiedTableName)));
            Map<String, ColumnHandle> allColumnHandles = metadata.getColumnHandles(session, tableHandle);
            return allColumnHandles;
        }

        private boolean isHiveCatalog(ColumnHandle columnHandle) throws ClassNotFoundException {
            return columnHandle.getClass().isAssignableFrom(Class.forName("io.prestosql.plugin.hive.HiveColumnHandle", false, columnHandle.getClass().getClassLoader()));
        }

        private boolean isPartitionKey(ColumnHandle columnHandle)
                throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
            Method method = columnHandle.getClass().getMethod("isPartitionKey");
            Boolean isPartitionKey = (Boolean) method.invoke(columnHandle);
            return isPartitionKey;
        }

        private Type getColumnType(ColumnHandle columnHandle)
                throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
            Method method = columnHandle.getClass().getMethod("getType");
            Type type = (Type) method.invoke(columnHandle);
            return type;
        }

    }

}
