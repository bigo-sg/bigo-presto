package io.prestosql.sql.rewrite;

import io.prestosql.Session;
import io.prestosql.execution.warnings.WarningCollector;
import io.prestosql.metadata.Metadata;
import io.prestosql.security.AccessControl;
import io.prestosql.sql.analyzer.QueryExplainer;
import io.prestosql.sql.parser.SqlParser;
import io.prestosql.sql.tree.*;

import java.util.*;

import static java.util.Objects.requireNonNull;

public class CubeRelatedRewrite implements StatementRewrite.Rewrite {

    @Override
    public Statement rewrite(Session session, Metadata metadata, SqlParser parser, Optional<QueryExplainer> queryExplainer,
                             Statement node, List<Expression> parameters, Map<NodeRef<Parameter>, Expression> parameterLookup,
                             AccessControl accessControl, WarningCollector warningCollector) {
        Visitor visitor = new Visitor(metadata, parser, session, accessControl);
        return (Statement) visitor.process(node);
    }

    public enum StatementType {

        WITH_CUBE("withCube"),
        ROLLUP("rollup"),
        GROUPING_SETS("groupingSets");

        private String typeName;

        StatementType(String typeName) {
            this.typeName = typeName;
        }

        public static StatementType getStatementTypeByName(String typeName) {
            if (null == typeName || "".equals(typeName)) {
                return null;
            }
            for (StatementType statementType : StatementType.values()) {
                if (statementType.typeName.equals(typeName)) {
                    return statementType;
                }
            }
            return null;
        }
    }

    private static class Visitor
            extends DefaultTraversalVisitor<Node, Void> {
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
        protected Node visitTableSubquery(TableSubquery node, Void context) {
            Node query = process(node.getQuery(), context);
            if (query instanceof Query) {
                node.setQuery((Query) query);
            }
            return node;
        }

        @Override
        protected Node visitLateral(Lateral node, Void context) {
            Node query = process(node.getQuery(), context);
            if (query instanceof Query) {
                node.setQuery((Query) query);
            }
            return super.visitLateral(node, context);
        }

        @Override
        protected Node visitCreateView(CreateView node, Void context) {
            Node query = process(node.getQuery(), context);
            if (query instanceof Query) {
                node.setQuery((Query) query);
            }
            return node;
        }

        @Override
        protected Node visitCreateTableAsSelect(CreateTableAsSelect node, Void context) {
            Node query = process(node.getQuery(), context);
            if (query instanceof Query) {
                node.setQuery((Query) query);
            }
            for (Property property : node.getProperties()) {
                process(property, context);
            }
            return node;
        }

        @Override
        protected Node visitInsert(Insert node, Void context) {
            Node query = process(node.getQuery(), context);
            if (query instanceof Query) {
                node.setQuery((Query) query);
            }
            return node;
        }

        @Override
        protected Node visitSubqueryExpression(SubqueryExpression node, Void context) {
            Node query = process(node.getQuery(), context);
            if (query instanceof Query) {
                node.setQuery((Query) query);
            }
            return node;
        }

        @Override
        protected Node visitWithQuery(WithQuery node, Void context) {
            Node query = process(node.getQuery(), context);
            if (query instanceof Query) {
                node.setQuery((Query) query);
            }
            return node;
        }

        @Override
        protected Node visitQuery(Query node, Void context) {
            if (null != node.getQueryBody()) {
                QueryBody queryBody = node.getQueryBody();
                if (queryBody instanceof QuerySpecification) {
                    QuerySpecification querySpecification = (QuerySpecification) queryBody;
                    if (querySpecification.getGroupBy().isPresent()) {
                        Query query = doRewrite(node);
                        return query;
                    }
                }
            }
            super.visitQuery(node, context);
            return node;
        }

        @Override
        protected Node visitNode(Node node, Void context) {
            return node;
        }

        private Query doRewrite(Query query) {
            Map<String, Expression> expressionMap = new HashMap<>();
            List<List<Expression>> groupingSets = new ArrayList<>();
            QuerySpecification querySpecification = (QuerySpecification) query.getQueryBody();
            GroupBy groupBy = querySpecification.getGroupBy().get();
            if (null == groupBy) {
                return query;
            }

            StatementType statementType = null;
            List<GroupingElement> groupingElements = groupBy.getGroupingElements();
            for (GroupingElement groupingElement : groupingElements) {
                if (groupingElement instanceof Cube || groupingElement instanceof Rollup) {
                    if (groupingElement instanceof Cube) {
                        statementType = StatementType.WITH_CUBE;
                    } else if (groupingElement instanceof Rollup) {
                        statementType = StatementType.ROLLUP;
                    }
                    List<Expression> expressions = groupingElement.getExpressions();
                    for (Expression expression : expressions) {
                        if (expression instanceof IfExpression || expression instanceof SearchedCaseExpression) {
                            // find the Expression, put it to the map
                            expressionMap.put(genRandomAlias(), expression);
                        }
                    }
                } else if (groupingElement instanceof GroupingSets) {
                    statementType = StatementType.GROUPING_SETS;
                    Map<Expression, String> expressionToAliasMap = new HashMap<>();
                    for (List<Expression> list : ((GroupingSets) groupingElement).getSets()) {
                        List<Expression> newList = new ArrayList<>();
                        for (Expression expression : list) {
                            if (expression instanceof IfExpression || expression instanceof SearchedCaseExpression) {
                                String alias = expressionToAliasMap.get(expression);
                                if (null == alias) {
                                    alias = genRandomAlias();
                                }
                                expressionMap.put(alias, expression);
                                expressionToAliasMap.put(expression, alias);
                                newList.add(new Identifier(alias));
                            }
                        }
                        groupingSets.add(newList);
                    }
                }
            }

            if (expressionMap.size() == 0) {
                return query;
            }

            // create extra subquery statement
            List<SelectItem> selectItems = new ArrayList<>();
            for (String alias : expressionMap.keySet()) {
                Expression expression = expressionMap.get(alias);
                SingleColumn singleColumn = new SingleColumn(expression, Optional.of(new Identifier(alias)));
                selectItems.add(singleColumn);
            }
            AllColumns allColumns = new AllColumns();
            selectItems.add(allColumns);
            Select select = new Select(false, selectItems);

            QuerySpecification subquerySpecification = new QuerySpecification(select, querySpecification.getFrom(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
            Query subQuery = new Query(Optional.empty(), subquerySpecification, Optional.empty(), Optional.empty(), Optional.empty());

            TableSubquery tableSubquery = new TableSubquery(subQuery);

            Identifier subqueryTableAlias = new Identifier(genRandomAlias());
            AliasedRelation extraSubquery = new AliasedRelation(tableSubquery, subqueryTableAlias, null);

            Query newQuery = setFromAndGroupBy(query, querySpecification, extraSubquery, expressionMap, statementType, groupingSets);
            return newQuery;
        }

        private Query setFromAndGroupBy(
                Query query,
                QuerySpecification querySpecification,
                AliasedRelation aliasedRelation,
                Map<String, Expression> expressionMap,
                StatementType statementType,
                List<List<Expression>> groupingSets) {
            // set the group by statement
            List<Expression> expressions = new ArrayList<>();
            for (String alias : expressionMap.keySet()) {
                Identifier identifier = new Identifier(alias);
                expressions.add(identifier);
            }

            List<GroupingElement> groupingElements = new ArrayList<>();
            switch(statementType) {
                case WITH_CUBE:
                    groupingElements.add(new Cube(expressions));
                    break;
                case ROLLUP:
                    groupingElements.add(new Rollup(expressions));
                case GROUPING_SETS:
                    groupingElements.add(new GroupingSets(groupingSets));
                    break;
            }

            GroupBy groupBy = new GroupBy(querySpecification.getGroupBy().get().isDistinct(), groupingElements);

            // replace selectItem here
            List<SelectItem> selectItems = new ArrayList<>();
            for (SelectItem selectItem : querySpecification.getSelect().getSelectItems()) {
                boolean hasReplaced = false;
                if (selectItem instanceof SingleColumn) {
                    SingleColumn singleColumn = (SingleColumn) selectItem;
                    for (String alias : expressionMap.keySet()) {
                        Expression expression = expressionMap.get(alias);
                        if (singleColumn.getExpression().equals(expression)) {
                            SingleColumn replacement = new SingleColumn(new Identifier(alias));
                            selectItems.add(replacement);
                            hasReplaced = true;
                            break;
                        }
                    }
                }
                if (!hasReplaced) {
                    selectItems.add(selectItem);
                }
            }
            Select replacedSelect = new Select(querySpecification.getSelect().isDistinct(), selectItems);


            QuerySpecification newQuerySpecification = new QuerySpecification(
                    replacedSelect,
                    Optional.of(aliasedRelation),
                    querySpecification.getWhere(),
                    Optional.of(groupBy),
                    querySpecification.getHaving(),
                    querySpecification.getOrderBy(),
                    querySpecification.getOffset(),
                    querySpecification.getLimit());
            Query newQuery = new Query(query.getWith(), newQuerySpecification, query.getOrderBy(), query.getOffset(), query.getLimit());
            return newQuery;
        }

        private String genRandomAlias() {
            String uuid = UUID.randomUUID().toString();
            int index = uuid.indexOf("-");
            String alias = uuid.substring(0, index);
            return alias;
        }

    }

}
