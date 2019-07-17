package io.prestosql.sql.parser;

import io.hivesql.sql.parser.SqlBaseParser;
import io.prestosql.sql.tree.Identifier;
import io.prestosql.sql.tree.Node;
import io.prestosql.sql.tree.NodeLocation;
import io.prestosql.sql.tree.Use;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.RuleNode;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

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
        return new Use(
                getLocation(ctx),
                visitIfPresent(ctx.catalog, Identifier.class),
                (Identifier) visit(ctx.db));
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
