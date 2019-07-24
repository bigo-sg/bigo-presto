package io.prestosql.sql.parser.hive;

import com.google.common.collect.Lists;
import io.airlift.log.Logger;
import io.hivesql.sql.parser.SqlBaseLexer;
import io.hivesql.sql.parser.SqlBaseParser;
import io.prestosql.sql.parser.ParsingException;
import io.prestosql.sql.parser.ParsingOptions;
import io.prestosql.sql.parser.debug.FieldUtils;
import io.prestosql.sql.tree.*;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.RuleNode;
import org.antlr.v4.runtime.tree.TerminalNode;

import java.text.NumberFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static io.prestosql.sql.parser.ParsingOptions.DecimalLiteralTreatment.*;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class HiveAstBuilder extends io.hivesql.sql.parser.SqlBaseBaseVisitor<Node> {
    private static final Logger LOG = Logger.get(HiveAstBuilder.class);

    private final ParsingOptions parsingOptions;

    public HiveAstBuilder(ParsingOptions parsingOptions)
    {
        this.parsingOptions = requireNonNull(parsingOptions, "parsingOptions is null");
    }

//    @Override
//    public Node visitChildren(RuleNode node) {
//        if (node.getChildCount() == 1) {
//            return node.getChild(0).accept(this);
//        } else {
//            return null;
//        }
//    }

    @Override
    protected Node aggregateResult(Node currentResult, Node nextResult) {
        if (currentResult == null) {
            return nextResult;
        }
        if (nextResult == null) {
            return currentResult;
        }

        return super.aggregateResult(currentResult, nextResult);
    }

    @Override
    public Node visitQueryOrganization(SqlBaseParser.QueryOrganizationContext ctx) {
        if (ctx.clusterBy != null && !ctx.clusterBy.isEmpty()) {
            throw new ParsingException("Don't support cluster by");
        }
        if (ctx.distributeBy != null && !ctx.distributeBy.isEmpty()) {
            throw new ParsingException("Don't support distribute by");
        }
        if (ctx.sort != null && !ctx.sort.isEmpty()) {
            throw new ParsingException("Don't support sort by");
        }

        return super.visitQueryOrganization(ctx);
    }

    @Override
    public Node visitAddTableColumns(SqlBaseParser.AddTableColumnsContext ctx) {
        return super.visitAddTableColumns(ctx);
    }

    @Override
    public Node visitAddTablePartition(SqlBaseParser.AddTablePartitionContext ctx) {
        return super.visitAddTablePartition(ctx);
    }

    @Override
    public Node visitAlterViewQuery(SqlBaseParser.AlterViewQueryContext ctx) {
        return super.visitAlterViewQuery(ctx);
    }

    @Override
    public Node visitAggregation(SqlBaseParser.AggregationContext ctx) {
        return super.visitAggregation(ctx);
    }

    @Override
    public Node visitAnalyze(SqlBaseParser.AnalyzeContext ctx) {
        return super.visitAnalyze(ctx);
    }

    @Override
    public Node visitBucketSpec(SqlBaseParser.BucketSpecContext ctx) {
        return super.visitBucketSpec(ctx);
    }

    @Override
    public Node visitCacheTable(SqlBaseParser.CacheTableContext ctx) {
        return super.visitCacheTable(ctx);
    }

    @Override
    public Node visitChangeColumn(SqlBaseParser.ChangeColumnContext ctx) {
        return super.visitChangeColumn(ctx);
    }

    @Override
    public Node visitClearCache(SqlBaseParser.ClearCacheContext ctx) {
        return super.visitClearCache(ctx);
    }

    @Override
    public Node visitAliasedQuery(SqlBaseParser.AliasedQueryContext ctx) {
        return super.visitAliasedQuery(ctx);
    }

    @Override
    public Node visitCreateDatabase(SqlBaseParser.CreateDatabaseContext ctx) {
        return super.visitCreateDatabase(ctx);
    }

    @Override
    public Node visitAliasedRelation(SqlBaseParser.AliasedRelationContext ctx) {
        return super.visitAliasedRelation(ctx);
    }

    @Override
    public Node visitConstantList(SqlBaseParser.ConstantListContext ctx) {
        return super.visitConstantList(ctx);
    }

    @Override
    public Node visitCreateFunction(SqlBaseParser.CreateFunctionContext ctx) {
        return super.visitCreateFunction(ctx);
    }

    @Override
    public Node visitCreateHiveTable(SqlBaseParser.CreateHiveTableContext ctx) {
        return super.visitCreateHiveTable(ctx);
    }

    @Override
    public Node visitArithmeticOperator(SqlBaseParser.ArithmeticOperatorContext ctx) {
        return super.visitArithmeticOperator(ctx);
    }

    @Override
    public Node visitCreateFileFormat(SqlBaseParser.CreateFileFormatContext ctx) {
        return super.visitCreateFileFormat(ctx);
    }

    @Override
    public Node visitCreateTableHeader(SqlBaseParser.CreateTableHeaderContext ctx) {
        return super.visitCreateTableHeader(ctx);
    }

    @Override
    public Node visitCreateTableLike(SqlBaseParser.CreateTableLikeContext ctx) {
        return super.visitCreateTableLike(ctx);
    }

    @Override
    public Node visitCreateTempViewUsing(SqlBaseParser.CreateTempViewUsingContext ctx) {
        return super.visitCreateTempViewUsing(ctx);
    }

    @Override
    public Node visitCreateView(SqlBaseParser.CreateViewContext ctx) {
        return super.visitCreateView(ctx);
    }

    @Override
    public Node visitBooleanValue(SqlBaseParser.BooleanValueContext ctx) {
        return super.visitBooleanValue(ctx);
    }

    @Override
    public Node visitColPosition(SqlBaseParser.ColPositionContext ctx) {
        return super.visitColPosition(ctx);
    }

    @Override
    public Node visitCtes(SqlBaseParser.CtesContext ctx) {
        return super.visitCtes(ctx);
    }

    @Override
    public Node visitDescribeColName(SqlBaseParser.DescribeColNameContext ctx) {
        return super.visitDescribeColName(ctx);
    }

    @Override
    public Node visitDescribeDatabase(SqlBaseParser.DescribeDatabaseContext ctx) {
        return super.visitDescribeDatabase(ctx);
    }

    @Override
    public Node visitDescribeFuncName(SqlBaseParser.DescribeFuncNameContext ctx) {
        return super.visitDescribeFuncName(ctx);
    }

    @Override
    public Node visitDescribeFunction(SqlBaseParser.DescribeFunctionContext ctx) {
        return super.visitDescribeFunction(ctx);
    }

    @Override
    public Node visitDescribeTable(SqlBaseParser.DescribeTableContext ctx) {
        return super.visitDescribeTable(ctx);
    }

    @Override
    public Node visitDropDatabase(SqlBaseParser.DropDatabaseContext ctx) {
        return super.visitDropDatabase(ctx);
    }

    @Override
    public Node visitDropFunction(SqlBaseParser.DropFunctionContext ctx) {
        return super.visitDropFunction(ctx);
    }

    @Override
    public Node visitDropTable(SqlBaseParser.DropTableContext ctx) {
        return super.visitDropTable(ctx);
    }

    @Override
    public Node visitRenameTable(SqlBaseParser.RenameTableContext ctx) {
        return super.visitRenameTable(ctx);
    }

    @Override
    public Node visitDropTablePartitions(SqlBaseParser.DropTablePartitionsContext ctx) {
        return super.visitDropTablePartitions(ctx);
    }

    @Override
    public Node visitExplain(SqlBaseParser.ExplainContext ctx) {
        return super.visitExplain(ctx);
    }

    @Override
    public Node visitFailNativeCommand(SqlBaseParser.FailNativeCommandContext ctx) {
        return super.visitFailNativeCommand(ctx);
    }

    @Override
    public Node visitFromClause(SqlBaseParser.FromClauseContext ctx) {
        return super.visitFromClause(ctx);
    }

    @Override
    public Node visitGenericFileFormat(SqlBaseParser.GenericFileFormatContext ctx) {
        return super.visitGenericFileFormat(ctx);
    }

    @Override
    public Node visitFunctionIdentifier(SqlBaseParser.FunctionIdentifierContext ctx) {
        return super.visitFunctionIdentifier(ctx);
    }

    @Override
    public Node visitInsertIntoTable(SqlBaseParser.InsertIntoTableContext ctx) {
        return super.visitInsertIntoTable(ctx);
    }

    @Override
    public Node visitGroupingSet(SqlBaseParser.GroupingSetContext ctx) {
        return super.visitGroupingSet(ctx);
    }

    @Override
    public Node visitHint(SqlBaseParser.HintContext ctx) {
        return super.visitHint(ctx);
    }

    @Override
    public Node visitHintStatement(SqlBaseParser.HintStatementContext ctx) {
        return super.visitHintStatement(ctx);
    }

    @Override
    public Node visitInlineTableDefault1(SqlBaseParser.InlineTableDefault1Context ctx) {
        return super.visitInlineTableDefault1(ctx);
    }

    @Override
    public Node visitInsertOverwriteDir(SqlBaseParser.InsertOverwriteDirContext ctx) {
        return super.visitInsertOverwriteDir(ctx);
    }

    @Override
    public Node visitInsertOverwriteHiveDir(SqlBaseParser.InsertOverwriteHiveDirContext ctx) {
        return super.visitInsertOverwriteHiveDir(ctx);
    }

    @Override
    public Node visitInsertOverwriteTable(SqlBaseParser.InsertOverwriteTableContext ctx) {
        return super.visitInsertOverwriteTable(ctx);
    }

    @Override
    public Node visitIdentifierCommentList(SqlBaseParser.IdentifierCommentListContext ctx) {
        return super.visitIdentifierCommentList(ctx);
    }

    @Override
    public Node visitIdentifierComment(SqlBaseParser.IdentifierCommentContext ctx) {
        return super.visitIdentifierComment(ctx);
    }

    @Override
    public Node visitJoinCriteria(SqlBaseParser.JoinCriteriaContext ctx) {
        return super.visitJoinCriteria(ctx);
    }

    @Override
    public Node visitRecoverPartitions(SqlBaseParser.RecoverPartitionsContext ctx) {
        return super.visitRecoverPartitions(ctx);
    }

    @Override
    public Node visitJoinRelation(SqlBaseParser.JoinRelationContext ctx) {
        return super.visitJoinRelation(ctx);
    }

    @Override
    public Node visitLoadData(SqlBaseParser.LoadDataContext ctx) {
        return super.visitLoadData(ctx);
    }

    @Override
    public Node visitManageResource(SqlBaseParser.ManageResourceContext ctx) {
        return super.visitManageResource(ctx);
    }

    @Override
    public Node visitIdentifierList(SqlBaseParser.IdentifierListContext ctx) {
        return super.visitIdentifierList(ctx);
    }

    @Override
    public Node visitComparisonOperator(SqlBaseParser.ComparisonOperatorContext ctx) {
        return super.visitComparisonOperator(ctx);
    }

    @Override
    public Node visitValueExpressionDefault(SqlBaseParser.ValueExpressionDefaultContext ctx) {
        return super.visitValueExpressionDefault(ctx);
    }

    @Override
    public Node visitNumericLiteral(SqlBaseParser.NumericLiteralContext ctx) {
        try {
            Number num = NumberFormat.getInstance().parse(ctx.getText());
            if (num instanceof Double) {
                switch (parsingOptions.getDecimalLiteralTreatment()) {
                    case AS_DOUBLE:
                        return new DoubleLiteral(getLocation(ctx), ctx.getText());
                    case AS_DECIMAL:
                        return new DecimalLiteral(getLocation(ctx), ctx.getText());
                    case REJECT:
                        throw new ParsingException("Unexpected decimal literal: " + ctx.getText());
                }
            } else if (num instanceof Integer || num instanceof Long) {
                return new LongLiteral(getLocation(ctx), ctx.getText());
            }
            else {
                throw new ParsingException("Can't parser number: " + ctx.getText());
            }
        } catch (ParseException e) {
            throw new ParsingException("Can't parser number: " + ctx.getText());
        }

        throw new ParsingException("Can't parser number: " + ctx.getText());
    }

    @Override
    public Node visitExpression(SqlBaseParser.ExpressionContext ctx) {
        return super.visitExpression(ctx);
    }

    @Override
    public Node visitLocationSpec(SqlBaseParser.LocationSpecContext ctx) {
        return super.visitLocationSpec(ctx);
    }

    @Override
    public Node visitMultiInsertQuery(SqlBaseParser.MultiInsertQueryContext ctx) {
        return super.visitMultiInsertQuery(ctx);
    }

    @Override
    public Node visitFunctionTable(SqlBaseParser.FunctionTableContext ctx) {
        return super.visitFunctionTable(ctx);
    }

    @Override
    public Node visitRefreshResource(SqlBaseParser.RefreshResourceContext ctx) {
        return super.visitRefreshResource(ctx);
    }

    @Override
    public Node visitIdentifierSeq(SqlBaseParser.IdentifierSeqContext ctx) {
        return super.visitIdentifierSeq(ctx);
    }

    @Override
    public Node visitLateralView(SqlBaseParser.LateralViewContext ctx) {
        return super.visitLateralView(ctx);
    }

    @Override
    public Node visitCreateTable(SqlBaseParser.CreateTableContext ctx) {
        return super.visitCreateTable(ctx);
    }

    @Override
    public Node visitConstantDefault(SqlBaseParser.ConstantDefaultContext ctx) {
        return super.visitConstantDefault(ctx);
    }

    @Override
    public Node visitStatementDefault(SqlBaseParser.StatementDefaultContext ctx) {
        return super.visitStatementDefault(ctx);
    }

    @Override
    public Node visitSingleInsertQuery(SqlBaseParser.SingleInsertQueryContext ctx) {
        QuerySpecification query = (QuerySpecification)visit(ctx.queryTerm());
        //TODO:
        Node todo = visit(ctx.queryOrganization());

        return new Query(
                getLocation(ctx),
                Optional.empty(),
                new QuerySpecification(
                        getLocation(ctx),
                        query.getSelect(),
                        query.getFrom(),
                        query.getWhere(),
                        query.getGroupBy(),
                        query.getHaving(),
                        Optional.empty(),//order by
                        Optional.empty(),//offset,
                        Optional.empty()//limit
                ),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
    }

    @Override
    public Node visitSingleDataType(SqlBaseParser.SingleDataTypeContext ctx) {
        return super.visitSingleDataType(ctx);
    }

    @Override
    public Node visitSingleStatement(SqlBaseParser.SingleStatementContext ctx) {
        return visit(ctx.statement());
    }

    @Override
    public Node visitQueryTermDefault(SqlBaseParser.QueryTermDefaultContext ctx) {
        return super.visitQueryTermDefault(ctx);
    }

    @Override
    public Node visitSingleExpression(SqlBaseParser.SingleExpressionContext ctx) {
        return visit(ctx.namedExpression());
    }

    @Override
    public Node visitNamedExpression(SqlBaseParser.NamedExpressionContext ctx) {
        NodeLocation nodeLocation = getLocation(ctx);
        Expression expression = (Expression)visit(ctx.expression());
        Optional<Identifier> identifier = visitIfPresent(ctx.identifier(), Identifier.class);

        return new SingleColumn(nodeLocation, expression, identifier);
    }

    @Override
    public Node visitUse(SqlBaseParser.UseContext ctx) {
        Use use;
        if (ctx.catalog != null) {
            use = new Use(
                    getLocation(ctx),
                    Optional.of(new Identifier(getLocation(ctx), ctx.catalog.getText(), false)),
                    new Identifier(getLocation(ctx), ctx.db.getText(), false));
            visit(ctx.catalog);
        } else {
            use = new Use(
                    getLocation(ctx),
                    Optional.ofNullable(null),
                    new Identifier(getLocation(ctx), ctx.db.getText(), false));
        }
        visit(ctx.db);
        return use;
    }

    @Override
    public Node visitSetSession(SqlBaseParser.SetSessionContext ctx) {

        SqlBaseParser.ExpressionContext expression = ctx.expression();
        SetSession setSession = new SetSession(getLocation(ctx),
                getQualifiedName(ctx.qualifiedName()), (Expression) visit(expression));
        LOG.info("-------setSession:" + FieldUtils.filedsToString(setSession));
        return setSession;
    }

    @Override
    public Node visitLogicalBinary(SqlBaseParser.LogicalBinaryContext ctx) {
        return new LogicalBinaryExpression(
                getLocation(ctx.operator),
                getLogicalBinaryOperator(ctx.operator),
                (Expression) visit(ctx.left),
                (Expression) visit(ctx.right));
    }

    @Override
    public Node visitQuerySpecification(SqlBaseParser.QuerySpecificationContext ctx) {
        if (ctx.kind.getType()  == SqlBaseParser.SELECT) {
            SqlBaseParser.NamedExpressionSeqContext namedExpressionSeqContext = ctx.namedExpressionSeq();
            List<SqlBaseParser.NamedExpressionContext> namedExpressionContexts =
                    namedExpressionSeqContext.namedExpression();

            List<SelectItem> selectItems = new ArrayList<>();
            for (SqlBaseParser.NamedExpressionContext namedExpressionContext: namedExpressionContexts) {
                SelectItem selectItem = (SelectItem)visit(namedExpressionContext);
                selectItems.add(selectItem);
            }

            NodeLocation nodeLocation = getLocation(ctx);
            Select select = new Select(getLocation(ctx.SELECT()), isDistinct(ctx.setQuantifier()), selectItems);

            Optional<Relation> from = Optional.empty();


            return new QuerySpecification(
                    nodeLocation,
                    select,
                    from,
                    visitIfPresent(ctx.where, Expression.class),
                    visitIfPresent(ctx.aggregation(), GroupBy.class),
                    visitIfPresent(ctx.having, Expression.class),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty());
        } else {
            throw new ParsingException("Don't support kind: " + ctx.kind.getText());
        }
    }

    private static boolean isDistinct(io.hivesql.sql.parser.SqlBaseParser.SetQuantifierContext setQuantifier)
    {
        return setQuantifier != null && setQuantifier.DISTINCT() != null;
    }

    @Override
    public Node visitLogicalNot(SqlBaseParser.LogicalNotContext ctx) {
        return new NotExpression(getLocation(ctx), (Expression) visit(ctx.booleanExpression()));
    }

    @Override
    public Node visitExists(SqlBaseParser.ExistsContext ctx) {
        return new ExistsPredicate(getLocation(ctx), new SubqueryExpression(getLocation(ctx), (Query) visit(ctx.query())));
    }

    @Override
    public Node visitComparison(SqlBaseParser.ComparisonContext ctx) {
        return new ComparisonExpression(
                getLocation(ctx.comparisonOperator()),
                getComparisonOperator(((TerminalNode) ctx.comparisonOperator().getChild(0)).getSymbol()),
                (Expression) visit(ctx.left),
                (Expression) visit(ctx.right));
    }

    @Override
    public Node visitPredicated(SqlBaseParser.PredicatedContext ctx) {
        if (ctx.predicate() != null) {
            return visit(ctx.predicate());
        }

        return visit(ctx.valueExpression());
    }

    @Override
    public Node visitArithmeticBinary(SqlBaseParser.ArithmeticBinaryContext ctx) {
        return new ArithmeticBinaryExpression(
                getLocation(ctx.operator),
                getArithmeticBinaryOperator(ctx.operator),
                (Expression) visit(ctx.left),
                (Expression) visit(ctx.right));
    }

    @Override
    public Node visitArithmeticUnary(SqlBaseParser.ArithmeticUnaryContext ctx) {
        Expression child = (Expression) visit(ctx.valueExpression());

        switch (ctx.operator.getType()) {
            case io.prestosql.sql.parser.SqlBaseLexer.MINUS:
                return ArithmeticUnaryExpression.negative(getLocation(ctx), child);
            case io.prestosql.sql.parser.SqlBaseLexer.PLUS:
                return ArithmeticUnaryExpression.positive(getLocation(ctx), child);
            default:
                throw new UnsupportedOperationException("Unsupported sign: " + ctx.operator.getText());
        }
    }

    // need to be implemented
    @Override
    public Node visitCast(SqlBaseParser.CastContext ctx) {
        LOG.info("---------- data type:" + ctx.dataType().getText());
        return new Cast(getLocation(ctx), (Expression) visit(ctx.expression()), getType(ctx.dataType()), false);
    }

    // need to be implemented
    @Override
    public Node visitStruct(SqlBaseParser.StructContext ctx) {
        return super.visitStruct(ctx);
    }
    // need to be implemented
    @Override
    public Node visitFirst(SqlBaseParser.FirstContext ctx) {
        return super.visitFirst(ctx);
    }

    // need to be implemented
    @Override
    public Node visitLast(SqlBaseParser.LastContext ctx) {
        return super.visitLast(ctx);
    }

    @Override
    public Node visitPosition(SqlBaseParser.PositionContext ctx) {
        List<Expression> arguments = Lists.reverse(visit(ctx.valueExpression(), Expression.class));
        return new FunctionCall(getLocation(ctx), QualifiedName.of("strpos"), arguments);
    }

    @Override
    public Node visitExtract(SqlBaseParser.ExtractContext ctx) {
        String fieldString = ctx.identifier().getText();
        Extract.Field field;
        try {
            field = Extract.Field.valueOf(fieldString.toUpperCase());
        }
        catch (IllegalArgumentException e) {
            throw parseError("Invalid EXTRACT field: " + fieldString, ctx);
        }
        return new Extract(getLocation(ctx), (Expression) visit(ctx.valueExpression()), field);
    }

    // to be implement: this is very complex!
    @Override
    public Node visitFunctionCall(SqlBaseParser.FunctionCallContext ctx) {
        return super.visitFunctionCall(ctx);
    }

    // to be implement: diff from presto!
    @Override
    public Node visitLambda(SqlBaseParser.LambdaContext ctx) {
        return super.visitLambda(ctx);
    }

    // to be implement: presto have no this func!
    @Override
    public Node visitWindowRef(SqlBaseParser.WindowRefContext ctx) {
        return super.visitWindowRef(ctx);
    }

    // to be implement: presto have no this func!
    @Override
    public Node visitWindowDef(SqlBaseParser.WindowDefContext ctx) {
        return super.visitWindowDef(ctx);
    }
    // to be implement: presto have no this func!
    @Override
    public Node visitFrameBound(SqlBaseParser.FrameBoundContext ctx) {
        return super.visitFrameBound(ctx);
    }

    @Override
    public Node visitRowConstructor(SqlBaseParser.RowConstructorContext ctx) {
        return new Row(getLocation(ctx), visit(ctx.namedExpression(), Expression.class));
    }

    @Override
    public Node visitSubqueryExpression(SqlBaseParser.SubqueryExpressionContext ctx) {
        return new SubqueryExpression(getLocation(ctx), (Query) visit(ctx.query()));
    }

    @Override
    public Node visitSimpleCase(SqlBaseParser.SimpleCaseContext ctx) {
        return new SimpleCaseExpression(
                getLocation(ctx),
                (Expression) visit(ctx.value),
                visit(ctx.whenClause(), WhenClause.class),
                visitIfPresent(ctx.elseExpression, Expression.class));
    }

    @Override
    public Node visitSearchedCase(SqlBaseParser.SearchedCaseContext ctx) {
        return new SearchedCaseExpression(
                getLocation(ctx),
                visit(ctx.whenClause(), WhenClause.class),
                visitIfPresent(ctx.elseExpression, Expression.class));
    }

    @Override
    public Node visitDereference(SqlBaseParser.DereferenceContext ctx) {
        return new DereferenceExpression(
                getLocation(ctx),
                (Expression) visit(ctx.base),
                (Identifier) visit(ctx.fieldName));
    }

    @Override
    public Node visitColumnReference(SqlBaseParser.ColumnReferenceContext ctx) {
        return visit(ctx.identifier());
    }

    @Override
    public Node visitSubscript(SqlBaseParser.SubscriptContext ctx) {
        return new SubscriptExpression(getLocation(ctx),
                (Expression) visit(ctx.value), (Expression) visit(ctx.index));
    }

    @Override
    public Node visitParenthesizedExpression(SqlBaseParser.ParenthesizedExpressionContext ctx) {
        return visit(ctx.expression());
    }

    @Override
    public Node visitSortItem(SqlBaseParser.SortItemContext ctx) {
        return new SortItem(
                getLocation(ctx),
                (Expression) visit(ctx.expression()),
                Optional.ofNullable(ctx.ordering)
                        .map(HiveAstBuilder::getOrderingType)
                        .orElse(SortItem.Ordering.ASCENDING),
                Optional.ofNullable(ctx.nullOrder)
                        .map(HiveAstBuilder::getNullOrderingType)
                        .orElse(SortItem.NullOrdering.UNDEFINED));
    }

    @Override
    public Node visitTypeConstructor(SqlBaseParser.TypeConstructorContext ctx) {

        String value = ((StringLiteral) visit(ctx.STRING())).getValue();

        String type = ctx.identifier().getText();
        if (type.equalsIgnoreCase("time")) {
            return new TimeLiteral(getLocation(ctx), value);
        }
        if (type.equalsIgnoreCase("timestamp")) {
            return new TimestampLiteral(getLocation(ctx), value);
        }
        if (type.equalsIgnoreCase("decimal")) {
            return new DecimalLiteral(getLocation(ctx), value);
        }
        if (type.equalsIgnoreCase("char")) {
            return new CharLiteral(getLocation(ctx), value);
        }

        return new GenericLiteral(getLocation(ctx), type, value);
    }

    @Override
    public Node visitNullLiteral(SqlBaseParser.NullLiteralContext ctx) {
        return new NullLiteral(getLocation(ctx));
    }

    // to be implement: presto have no this func!
    @Override
    public Node visitBooleanLiteral(SqlBaseParser.BooleanLiteralContext ctx) {
        return new BooleanLiteral(getLocation(ctx), ctx.getText());
    }

    @Override
    public Node visitIntegerLiteral(SqlBaseParser.IntegerLiteralContext ctx) {
        return new LongLiteral(getLocation(ctx), ctx.getText());
    }

    @Override
    public Node visitDecimalLiteral(SqlBaseParser.DecimalLiteralContext ctx) {
        return new DoubleLiteral(getLocation(ctx), ctx.getText());
    }

    // to be implement: presto have no this func!
    @Override
    public Node visitTinyIntLiteral(SqlBaseParser.TinyIntLiteralContext ctx) {
        return super.visitTinyIntLiteral(ctx);
    }

    // to be implement: presto have no this func!
    @Override
    public Node visitSmallIntLiteral(SqlBaseParser.SmallIntLiteralContext ctx) {
        return super.visitSmallIntLiteral(ctx);
    }

    // to be implement: presto have no this func!
    @Override
    public Node visitBigIntLiteral(SqlBaseParser.BigIntLiteralContext ctx) {
        return super.visitBigIntLiteral(ctx);
    }

    @Override
    public Node visitDoubleLiteral(SqlBaseParser.DoubleLiteralContext ctx) {
        return new DoubleLiteral(getLocation(ctx), ctx.getText());
    }

    // to be implement: presto have no this func!
    @Override
    public Node visitBigDecimalLiteral(SqlBaseParser.BigDecimalLiteralContext ctx) {
        return super.visitBigDecimalLiteral(ctx);
    }

    @Override
    public Node visitStringLiteral(SqlBaseParser.StringLiteralContext ctx) {
        return new StringLiteral(getLocation(ctx), unquote(ctx.getText()));
    }

    // to be implement: presto diff from hive!
    @Override
    public Node visitInterval(SqlBaseParser.IntervalContext ctx) {
        return super.visitInterval(ctx);
    }

    // to be implement: presto have no this func!
    @Override
    public Node visitIntervalField(SqlBaseParser.IntervalFieldContext ctx) {
        return super.visitIntervalField(ctx);
    }

    // to be implement: presto have no this func!
    @Override
    public Node visitPrimitiveDataType(SqlBaseParser.PrimitiveDataTypeContext ctx) {
        return super.visitPrimitiveDataType(ctx);
    }

    // to be implement: presto have no this func!
    @Override
    public Node visitComplexDataType(SqlBaseParser.ComplexDataTypeContext ctx) {
        return super.visitComplexDataType(ctx);
    }

    // to be implement: presto have no this func!
    @Override
    public Node visitColTypeList(SqlBaseParser.ColTypeListContext ctx) {
        return super.visitColTypeList(ctx);
    }

    // to be implement: presto have no this func!
    @Override
    public Node visitColType(SqlBaseParser.ColTypeContext ctx) {
        return super.visitColType(ctx);
    }

    // to be implement: presto have no this func!
    @Override
    public Node visitComplexColTypeList(SqlBaseParser.ComplexColTypeListContext ctx) {
        return super.visitComplexColTypeList(ctx);
    }

    // to be implement: presto have no this func!
    @Override
    public Node visitComplexColType(SqlBaseParser.ComplexColTypeContext ctx) {
        return super.visitComplexColType(ctx);
    }


    /////////////////////
    // Utility methods //
    /////////////////////
    private static SortItem.Ordering getOrderingType(Token token)
    {
        switch (token.getType()) {
            case SqlBaseLexer.ASC:
                return SortItem.Ordering.ASCENDING;
            case SqlBaseLexer.DESC:
                return SortItem.Ordering.DESCENDING;
        }

        throw new IllegalArgumentException("Unsupported ordering: " + token.getText());
    }

    private static SortItem.NullOrdering getNullOrderingType(Token token)
    {
        switch (token.getType()) {
            case SqlBaseLexer.FIRST:
                return SortItem.NullOrdering.FIRST;
            case SqlBaseLexer.LAST:
                return SortItem.NullOrdering.LAST;
        }

        throw new IllegalArgumentException("Unsupported ordering: " + token.getText());
    }

    private static ParsingException parseError(String message, ParserRuleContext context)
    {
        return new ParsingException(message, null, context.getStart().getLine(), context.getStart().getCharPositionInLine());
    }

    private String getType(SqlBaseParser.DataTypeContext type)
    {
        throw new IllegalArgumentException("Unsupported type specification: " + type.getText());
    }

    private static ArithmeticBinaryExpression.Operator getArithmeticBinaryOperator(Token operator)
    {
        switch (operator.getType()) {
            case io.hivesql.sql.parser.SqlBaseLexer.PLUS:
                return ArithmeticBinaryExpression.Operator.ADD;
            case io.hivesql.sql.parser.SqlBaseLexer.MINUS:
                return ArithmeticBinaryExpression.Operator.SUBTRACT;
            case io.hivesql.sql.parser.SqlBaseLexer.ASTERISK:
                return ArithmeticBinaryExpression.Operator.MULTIPLY;
            case io.hivesql.sql.parser.SqlBaseLexer.SLASH:
                return ArithmeticBinaryExpression.Operator.DIVIDE;
            case io.hivesql.sql.parser.SqlBaseLexer.PERCENT:
                return ArithmeticBinaryExpression.Operator.MODULUS;
            case io.hivesql.sql.parser.SqlBaseLexer.DIV:
                return ArithmeticBinaryExpression.Operator.DIV;
            case io.hivesql.sql.parser.SqlBaseLexer.TILDE:
                return ArithmeticBinaryExpression.Operator.TILDE;
            case io.hivesql.sql.parser.SqlBaseLexer.AMPERSAND:
                return ArithmeticBinaryExpression.Operator.AMPERSAND;
            case io.hivesql.sql.parser.SqlBaseLexer.CONCAT_PIPE:
                return ArithmeticBinaryExpression.Operator.CONCAT_PIPE;
            case io.hivesql.sql.parser.SqlBaseLexer.HAT:
                return ArithmeticBinaryExpression.Operator.HAT;
        }

        throw new UnsupportedOperationException("Unsupported operator: " + operator.getText());
    }

    private static ComparisonExpression.Operator getComparisonOperator(Token symbol)
    {
        switch (symbol.getType()) {
            case io.hivesql.sql.parser.SqlBaseLexer.EQ:
                return ComparisonExpression.Operator.EQUAL;
            case io.hivesql.sql.parser.SqlBaseLexer.NSEQ:
                return ComparisonExpression.Operator.EQNSF;
            case io.hivesql.sql.parser.SqlBaseLexer.NEQ:
                return ComparisonExpression.Operator.NOT_EQUAL;
            case io.hivesql.sql.parser.SqlBaseLexer.NEQJ:
                return ComparisonExpression.Operator.NOT_EQUAL;
            case io.hivesql.sql.parser.SqlBaseLexer.LT:
                return ComparisonExpression.Operator.LESS_THAN;
            case io.hivesql.sql.parser.SqlBaseLexer.LTE:
                return ComparisonExpression.Operator.LESS_THAN_OR_EQUAL;
            case io.hivesql.sql.parser.SqlBaseLexer.GT:
                return ComparisonExpression.Operator.GREATER_THAN;
            case io.hivesql.sql.parser.SqlBaseLexer.GTE:
                return ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL;
        }

        throw new IllegalArgumentException("Unsupported operator: " + symbol.getText());
    }

    private static LogicalBinaryExpression.Operator getLogicalBinaryOperator(Token token)
    {
        switch (token.getType()) {
            case io.hivesql.sql.parser.SqlBaseLexer.AND:
                return LogicalBinaryExpression.Operator.AND;
            case io.hivesql.sql.parser.SqlBaseLexer.OR:
                return LogicalBinaryExpression.Operator.OR;
        }

        throw new IllegalArgumentException("Unsupported operator: " + token.getText());
    }

    private QualifiedName getQualifiedName(SqlBaseParser.QualifiedNameContext context)
    {
        List<Identifier> identifiers = new ArrayList<>();
        for (SqlBaseParser.IdentifierContext identifierContext: context.identifier()) {
            Identifier identifier =
                    new Identifier(getLocation(identifierContext),
                            identifierContext.getText(), false);
            identifiers.add(identifier);
            visit(identifierContext);
        }
        return QualifiedName.of(identifiers);
    }

    private <T> List<T> visit(List<? extends ParserRuleContext> contexts, Class<T> clazz)
    {
        return contexts.stream()
                .map(this::visit)
                .map(clazz::cast)
                .collect(toList());
    }

    public Node visitUnquotedIdentifier(io.prestosql.sql.parser.SqlBaseParser.UnquotedIdentifierContext context)
    {
        return new Identifier(getLocation(context), context.getText(), false);
    }

    private <T> Optional<T> visitIfPresent(ParserRuleContext context, Class<T> clazz)
    {
        return Optional.ofNullable(context)
                .map(this::visit)
                .map(clazz::cast);
    }

    public static NodeLocation getLocation(ParserRuleContext parserRuleContext)
    {
        requireNonNull(parserRuleContext, "parserRuleContext is null");
        return getLocation(parserRuleContext.getStart());
    }

    public static NodeLocation getLocation(TerminalNode terminalNode)
    {
        requireNonNull(terminalNode, "terminalNode is null");
        return getLocation(terminalNode.getSymbol());
    }

    public static NodeLocation getLocation(Token token)
    {
        requireNonNull(token, "token is null");
        return new NodeLocation(token.getLine(), token.getCharPositionInLine());
    }

    private static String unquote(String value)
    {
        return value.substring(1, value.length() - 1)
                .replace("''", "'");
    }
}
