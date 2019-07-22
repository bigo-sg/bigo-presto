package io.prestosql.sql.parser;

import com.google.common.collect.Lists;
import io.airlift.log.Logger;
import io.hivesql.sql.parser.SqlBaseLexer;
import io.hivesql.sql.parser.SqlBaseParser;
import io.prestosql.sql.parser.debug.FieldUtils;
import io.prestosql.sql.tree.*;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.RuleNode;
import org.antlr.v4.runtime.tree.TerminalNode;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

/**
 * @author tangyun@bigo.sg
 * @date 7/17/19 12:07 PM
 */
public class HiveAstBuilder extends io.hivesql.sql.parser.SqlBaseBaseVisitor<Node> {


    private static final Logger LOG = Logger.get(HiveAstBuilder.class);


//    @Override
//    public Node visitChildren(RuleNode node) {
//        if (node.getChildCount() == 1) {
//            return node.getChild(0).accept(this);
//        } else {
//            return null;
//        }
//    }

    @Override
    public Node visitSingleStatement(SqlBaseParser.SingleStatementContext ctx) {
        return visit(ctx.statement());
    }

    @Override
    public Node visitSingleExpression(SqlBaseParser.SingleExpressionContext ctx) {
        return visit(ctx.namedExpression());
    }

    @Override
    public Node visitNamedExpression(SqlBaseParser.NamedExpressionContext ctx) {

        SqlBaseParser.IdentifierContext identifierContext = ctx.identifier();
        SqlBaseParser.IdentifierListContext identifierListContext = ctx.identifierList();
        TerminalNode terminalNode = ctx.AS();
        SqlBaseParser.ExpressionContext expressionContext = ctx.expression();
        return visit(ctx.expression());
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
//        Optional<Relation> from = Optional.empty();
//        List<SelectItem> selectItems = visit(context.selectItem(), SelectItem.class);
//
//        List<Relation> relations = visit(context.relation(), Relation.class);
//        if (!relations.isEmpty()) {
//            // synthesize implicit join nodes
//            Iterator<Relation> iterator = relations.iterator();
//            Relation relation = iterator.next();
//
//            while (iterator.hasNext()) {
//                relation = new Join(getLocation(context), Join.Type.IMPLICIT, relation, iterator.next(), Optional.empty());
//            }
//
//            from = Optional.of(relation);
//        }

//        return new QuerySpecification(
//                getLocation(context),
//                new Select(getLocation(context.SELECT()), isDistinct(context.setQuantifier()), selectItems),
//                from,
//                visitIfPresent(context.where, Expression.class),
//                visitIfPresent(context.groupBy(), GroupBy.class),
//                visitIfPresent(context.having, Expression.class),
//                Optional.empty(),
//                Optional.empty(),
//                Optional.empty());

        SqlBaseParser.NamedExpressionSeqContext namedExpressionSeqContext = ctx.namedExpressionSeq();
        List<SqlBaseParser.NamedExpressionContext> namedExpressionContexts =
                namedExpressionSeqContext.namedExpression();

        List<SelectItem> selectItems = new ArrayList<>();
        for (SqlBaseParser.NamedExpressionContext namedExpressionContext: namedExpressionContexts) {
            SelectItem selectItem = new SingleColumn((Expression)visit(namedExpressionContext));
            selectItems.add(selectItem);
        }

        NodeLocation nodeLocation = getLocation(ctx);
        Select select = new Select(getLocation(ctx.SELECT()), isDistinct(ctx.setQuantifier()), selectItems);
        Optional<Relation> from = Optional.empty();

        TerminalNode where = ctx.WHERE();
        visit(ctx.namedExpressionSeq());

        return super.visitQuerySpecification(ctx);
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

    // to be implement: presto have no this func!
    @Override
    public Node visitStringLiteral(SqlBaseParser.StringLiteralContext ctx) {
        return super.visitStringLiteral(ctx);
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

}
