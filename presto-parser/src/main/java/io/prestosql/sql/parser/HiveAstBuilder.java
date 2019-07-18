package io.prestosql.sql.parser;

import io.hivesql.sql.parser.SqlBaseLexer;
import io.hivesql.sql.parser.SqlBaseParser;
import io.prestosql.sql.tree.*;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.RuleNode;
import org.antlr.v4.runtime.tree.TerminalNode;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

/**
 * @author tangyun@bigo.sg
 * @date 7/17/19 12:07 PM
 */
public class HiveAstBuilder extends io.hivesql.sql.parser.SqlBaseBaseVisitor<Node> {

    @Override
    public Node visitChildren(RuleNode node) {
        if (node.getChildCount() == 1) {
            return node.getChild(0).accept(this);
        } else {
            return null;
        }
    }

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
        return visit(ctx.expression());
    }

    @Override
    public Node visitUse(SqlBaseParser.UseContext ctx) {

        Use use = new Use(
                getLocation(ctx),
                visitIfPresent(ctx.catalog, Identifier.class),
                new Identifier(getLocation(ctx), ctx.db.getText(), false));
        visit(ctx.db);
        return use;
    }

    @Override
    public Node visitSetSession(SqlBaseParser.SetSessionContext ctx) {
        return new SetSession(getLocation(ctx), getQualifiedName(ctx.qualifiedName()), (Expression) visit(ctx.expression()));
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


    @Override
    public Node visitCast(SqlBaseParser.CastContext ctx) {
        return visit(ctx);
//        return new Cast(getLocation(ctx), (Expression) visit(ctx.expression()), getType(ctx.dataType()), false);
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
            case SqlBaseLexer.NSEQ:
                return ComparisonExpression.Operator.EQNSF;
            case io.hivesql.sql.parser.SqlBaseLexer.NEQ | io.hivesql.sql.parser.SqlBaseLexer.NEQJ:
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
        return QualifiedName.of(visit(context.identifier(), Identifier.class));
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

    public static NodeLocation getLocation(Token token)
    {
        requireNonNull(token, "token is null");
        return new NodeLocation(token.getLine(), token.getCharPositionInLine());
    }

}
