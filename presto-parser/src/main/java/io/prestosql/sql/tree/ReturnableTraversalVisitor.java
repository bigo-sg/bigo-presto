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
package io.prestosql.sql.tree;

import io.prestosql.sql.parser.hive.CreateTableLike;
import io.prestosql.sql.parser.hive.LoadData;
import io.prestosql.sql.parser.hive.RLikePredicate;

public abstract class ReturnableTraversalVisitor
        extends AstVisitor<Node, Void>
{
    @Override
    protected Node visitExtract(Extract node, Void context)
    {
        return process(node.getExpression(), context);
    }

    @Override
    protected Node visitCast(Cast node, Void context)
    {
        return process(node.getExpression(), context);
    }

    @Override
    protected Node visitArithmeticBinary(ArithmeticBinaryExpression node, Void context)
    {
        process(node.getLeft(), context);
        process(node.getRight(), context);

        return node;
    }

    @Override
    protected Node visitBetweenPredicate(BetweenPredicate node, Void context)
    {
        process(node.getValue(), context);
        process(node.getMin(), context);
        process(node.getMax(), context);

        return node;
    }

    @Override
    protected Node visitCoalesceExpression(CoalesceExpression node, Void context)
    {
        for (Expression operand : node.getOperands()) {
            process(operand, context);
        }

        return node;
    }

    @Override
    protected Node visitAtTimeZone(AtTimeZone node, Void context)
    {
        process(node.getValue(), context);
        process(node.getTimeZone(), context);

        return node;
    }

    @Override
    protected Node visitArrayConstructor(ArrayConstructor node, Void context)
    {
        for (Expression expression : node.getValues()) {
            process(expression, context);
        }

        return node;
    }

    @Override
    protected Node visitSubscriptExpression(SubscriptExpression node, Void context)
    {
        process(node.getBase(), context);
        process(node.getIndex(), context);

        return node;
    }

    @Override
    protected Node visitComparisonExpression(ComparisonExpression node, Void context)
    {
        process(node.getLeft(), context);
        process(node.getRight(), context);

        return node;
    }

    @Override
    protected Node visitQuery(Query node, Void context)
    {
        if (node.getWith().isPresent()) {
            process(node.getWith().get(), context);
        }
        process(node.getQueryBody(), context);
        if (node.getOrderBy().isPresent()) {
            process(node.getOrderBy().get(), context);
        }

        return node;
    }

    @Override
    protected Node visitWith(With node, Void context)
    {
        for (WithQuery query : node.getQueries()) {
            process(query, context);
        }

        return node;
    }

    @Override
    protected Node visitWithQuery(WithQuery node, Void context)
    {
        return process(node.getQuery(), context);
    }

    @Override
    protected Node visitSelect(Select node, Void context)
    {
        for (SelectItem item : node.getSelectItems()) {
            process(item, context);
        }

        return node;
    }

    @Override
    protected Node visitSingleColumn(SingleColumn node, Void context)
    {
        process(node.getExpression(), context);

        return node;
    }

    @Override
    protected Node visitAllColumns(AllColumns node, Void context)
    {
        node.getTarget().ifPresent(value -> process(value, context));

        return node;
    }

    @Override
    protected Node visitWhenClause(WhenClause node, Void context)
    {
        process(node.getOperand(), context);
        process(node.getResult(), context);

        return node;
    }

    @Override
    protected Node visitInPredicate(InPredicate node, Void context)
    {
        process(node.getValue(), context);
        process(node.getValueList(), context);

        return node;
    }

    @Override
    protected Node visitFunctionCall(FunctionCall node, Void context)
    {
        for (Expression argument : node.getArguments()) {
            process(argument, context);
        }

        if (node.getOrderBy().isPresent()) {
            process(node.getOrderBy().get(), context);
        }

        if (node.getWindow().isPresent()) {
            process(node.getWindow().get(), context);
        }

        if (node.getFilter().isPresent()) {
            process(node.getFilter().get(), context);
        }

        return node;
    }

    @Override
    protected Node visitGroupingOperation(GroupingOperation node, Void context)
    {
        for (Expression columnArgument : node.getGroupingColumns()) {
            process(columnArgument, context);
        }

        return node;
    }

    @Override
    protected Node visitDereferenceExpression(DereferenceExpression node, Void context)
    {
        process(node.getBase(), context);
        return node;
    }

    @Override
    public Node visitWindow(Window node, Void context)
    {
        for (Expression expression : node.getPartitionBy()) {
            process(expression, context);
        }

        if (node.getOrderBy().isPresent()) {
            process(node.getOrderBy().get(), context);
        }

        if (node.getFrame().isPresent()) {
            process(node.getFrame().get(), context);
        }

        return node;
    }

    @Override
    public Node visitWindowFrame(WindowFrame node, Void context)
    {
        process(node.getStart(), context);
        if (node.getEnd().isPresent()) {
            process(node.getEnd().get(), context);
        }

        return node;
    }

    @Override
    public Node visitFrameBound(FrameBound node, Void context)
    {
        if (node.getValue().isPresent()) {
            process(node.getValue().get(), context);
        }

        return node;
    }

    @Override
    protected Node visitSimpleCaseExpression(SimpleCaseExpression node, Void context)
    {
        process(node.getOperand(), context);
        for (WhenClause clause : node.getWhenClauses()) {
            process(clause, context);
        }

        node.getDefaultValue()
                .ifPresent(value -> process(value, context));

        return node;
    }

    @Override
    protected Node visitInListExpression(InListExpression node, Void context)
    {
        for (Expression value : node.getValues()) {
            process(value, context);
        }

        return node;
    }

    @Override
    protected Node visitNullIfExpression(NullIfExpression node, Void context)
    {
        process(node.getFirst(), context);
        process(node.getSecond(), context);

        return node;
    }

    @Override
    protected Node visitIfExpression(IfExpression node, Void context)
    {
        process(node.getCondition(), context);
        process(node.getTrueValue(), context);
        if (node.getFalseValue().isPresent()) {
            process(node.getFalseValue().get(), context);
        }

        return node;
    }

    @Override
    protected Node visitTryExpression(TryExpression node, Void context)
    {
        process(node.getInnerExpression(), context);
        return node;
    }

    @Override
    protected Node visitBindExpression(BindExpression node, Void context)
    {
        for (Expression value : node.getValues()) {
            process(value, context);
        }
        process(node.getFunction(), context);

        return node;
    }

    @Override
    protected Node visitArithmeticUnary(ArithmeticUnaryExpression node, Void context)
    {
        return process(node.getValue(), context);
    }

    @Override
    protected Node visitNotExpression(NotExpression node, Void context)
    {
        return process(node.getValue(), context);
    }

    @Override
    protected Node visitSearchedCaseExpression(SearchedCaseExpression node, Void context)
    {
        for (WhenClause clause : node.getWhenClauses()) {
            process(clause, context);
        }
        node.getDefaultValue()
                .ifPresent(value -> process(value, context));

        return node;
    }

    @Override
    protected Node visitLikePredicate(LikePredicate node, Void context)
    {
        process(node.getValue(), context);
        process(node.getPattern(), context);
        node.getEscape().ifPresent(value -> process(value, context));

        return node;
    }

    @Override
    public Node visitRLikePredicate(RLikePredicate node, Void context)
    {
        process(node.getValue(), context);
        process(node.getPattern(), context);
        node.getEscape().ifPresent(value -> process(value, context));

        return node;
    }

    @Override
    protected Node visitIsNotNullPredicate(IsNotNullPredicate node, Void context)
    {
        return process(node.getValue(), context);
    }

    @Override
    protected Node visitIsNullPredicate(IsNullPredicate node, Void context)
    {
        return process(node.getValue(), context);
    }

    @Override
    protected Node visitLogicalBinaryExpression(LogicalBinaryExpression node, Void context)
    {
        process(node.getLeft(), context);
        process(node.getRight(), context);

        return node;
    }

    @Override
    protected Node visitSubqueryExpression(SubqueryExpression node, Void context)
    {
        return process(node.getQuery(), context);
    }

    @Override
    protected Node visitOrderBy(OrderBy node, Void context)
    {
        for (SortItem sortItem : node.getSortItems()) {
            process(sortItem, context);
        }
        return node;
    }

    @Override
    protected Node visitSortItem(SortItem node, Void context)
    {
        return process(node.getSortKey(), context);
    }

    @Override
    protected Node visitQuerySpecification(QuerySpecification node, Void context)
    {
        process(node.getSelect(), context);
        if (node.getFrom().isPresent()) {
            process(node.getFrom().get(), context);
        }
        if (node.getWhere().isPresent()) {
            process(node.getWhere().get(), context);
        }
        if (node.getGroupBy().isPresent()) {
            process(node.getGroupBy().get(), context);
        }
        if (node.getHaving().isPresent()) {
            process(node.getHaving().get(), context);
        }
        if (node.getOrderBy().isPresent()) {
            process(node.getOrderBy().get(), context);
        }
        return node;
    }

    @Override
    protected Node visitSetOperation(SetOperation node, Void context)
    {
        for (Relation relation : node.getRelations()) {
            process(relation, context);
        }
        return node;
    }

    @Override
    protected Node visitValues(Values node, Void context)
    {
        for (Expression row : node.getRows()) {
            process(row, context);
        }
        return node;
    }

    @Override
    protected Node visitRow(Row node, Void context)
    {
        for (Expression expression : node.getItems()) {
            process(expression, context);
        }
        return node;
    }

    @Override
    protected Node visitTableSubquery(TableSubquery node, Void context)
    {
        return process(node.getQuery(), context);
    }

    @Override
    protected Node visitAliasedRelation(AliasedRelation node, Void context)
    {
        return process(node.getRelation(), context);
    }

    @Override
    protected Node visitSampledRelation(SampledRelation node, Void context)
    {
        process(node.getRelation(), context);
        process(node.getSamplePercentage(), context);
        return node;
    }

    @Override
    protected Node visitJoin(Join node, Void context)
    {
        process(node.getLeft(), context);
        process(node.getRight(), context);

        node.getCriteria()
                .filter(criteria -> criteria instanceof JoinOn)
                .map(criteria -> process(((JoinOn) criteria).getExpression(), context));

        return node;
    }

    @Override
    protected Node visitUnnest(Unnest node, Void context)
    {
        for (Expression expression : node.getExpressions()) {
            process(expression, context);
        }

        return node;
    }

    @Override
    protected Node visitGroupBy(GroupBy node, Void context)
    {
        for (GroupingElement groupingElement : node.getGroupingElements()) {
            process(groupingElement, context);
        }

        return node;
    }

    @Override
    protected Node visitCube(Cube node, Void context)
    {
        return node;
    }

    @Override
    protected Node visitRollup(Rollup node, Void context)
    {
        return node;
    }

    @Override
    protected Node visitSimpleGroupBy(SimpleGroupBy node, Void context)
    {
        for (Expression expression : node.getExpressions()) {
            process(expression, context);
        }

        return node;
    }

    @Override
    protected Node visitGroupingSets(GroupingSets node, Void context)
    {
        return node;
    }

    @Override
    protected Node visitInsert(Insert node, Void context)
    {
        process(node.getQuery(), context);

        return node;
    }

    @Override
    protected Node visitDelete(Delete node, Void context)
    {
        process(node.getTable(), context);
        node.getWhere().ifPresent(where -> process(where, context));

        return node;
    }

    @Override
    protected Node visitCreateTableAsSelect(CreateTableAsSelect node, Void context)
    {
        process(node.getQuery(), context);
        for (Property property : node.getProperties()) {
            process(property, context);
        }

        return node;
    }

    @Override
    protected Node visitProperty(Property node, Void context)
    {
        process(node.getName(), context);
        process(node.getValue(), context);

        return node;
    }

    @Override
    protected Node visitAnalyze(Analyze node, Void context)
    {
        for (Property property : node.getProperties()) {
            process(property, context);
        }
        return node;
    }

    @Override
    protected Node visitCreateView(CreateView node, Void context)
    {
        process(node.getQuery(), context);

        return node;
    }

    @Override
    protected Node visitSetSession(SetSession node, Void context)
    {
        process(node.getValue(), context);

        return node;
    }

    @Override
    protected Node visitAddColumn(AddColumn node, Void context)
    {
        process(node.getColumn(), context);

        return node;
    }

    @Override
    protected Node visitCreateTable(CreateTable node, Void context)
    {
        for (TableElement tableElement : node.getElements()) {
            process(tableElement, context);
        }
        for (Property property : node.getProperties()) {
            process(property, context);
        }

        return node;
    }

    @Override
    public Node visitCreateTableLike(CreateTableLike node, Void context)
    {
        return node;
    }

    @Override
    public Node visitLoadData(LoadData node, Void context)
    {
        return node;
    }

    @Override
    protected Node visitStartTransaction(StartTransaction node, Void context)
    {
        for (TransactionMode transactionMode : node.getTransactionModes()) {
            process(transactionMode, context);
        }

        return node;
    }

    @Override
    protected Node visitExplain(Explain node, Void context)
    {
        process(node.getStatement(), context);

        for (ExplainOption option : node.getOptions()) {
            process(option, context);
        }

        return node;
    }

    @Override
    protected Node visitQuantifiedComparisonExpression(QuantifiedComparisonExpression node, Void context)
    {
        process(node.getValue(), context);
        process(node.getSubquery(), context);

        return node;
    }

    @Override
    protected Node visitExists(ExistsPredicate node, Void context)
    {
        process(node.getSubquery(), context);

        return node;
    }

    @Override
    protected Node visitLateral(Lateral node, Void context)
    {
        process(node.getQuery(), context);

        return super.visitLateral(node, context);
    }
}
