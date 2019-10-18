package io.prestosql.sql.rewrite;

import io.prestosql.Session;
import io.prestosql.SystemSessionProperties;
import io.prestosql.execution.warnings.WarningCollector;
import io.prestosql.metadata.Metadata;
import io.prestosql.security.AccessControl;
import io.prestosql.sql.analyzer.QueryExplainer;
import io.prestosql.sql.parser.SqlParser;
import io.prestosql.sql.tree.CreateTableAsSelect;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.Identifier;
import io.prestosql.sql.tree.Limit;
import io.prestosql.sql.tree.Node;
import io.prestosql.sql.tree.Property;
import io.prestosql.sql.tree.QualifiedName;
import io.prestosql.sql.tree.Query;
import io.prestosql.sql.tree.QueryBody;
import io.prestosql.sql.tree.QuerySpecification;
import io.prestosql.sql.tree.Statement;
import io.prestosql.sql.tree.StringLiteral;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

final public class DownloadRewrite
        implements StatementRewrite.Rewrite {
    static final String RESULT_TABLE_NAME_PREFIX = "presto_";

    static final Optional<String> COMMENT = Optional.of("This table is generated automatically by Presto to store temporary result for download. It is safe to delete.");

    @Override
    // note, we didn't use visitor pattern in here as it's easier to implement in this way.
    public Statement rewrite(
            Session session,
            Metadata metadata,
            SqlParser parser,
            Optional<QueryExplainer> queryExplainer,
            Statement node,
            List<Expression> parameters,
            AccessControl accessControl,
            WarningCollector warningCollector) {

        if (! SystemSessionProperties.isEnableDownloadRewrite(session)) {
            return node;
        }

        // only rewrite query statement
        if (! (node instanceof Query)) {
            return node;
        }

        // only rewrite the query if this is the original statement.
        if (! node.getLocation().isPresent()) {
            return node;
        }

        Query query = updateQuery((Query) node, session);

        // use query Id(append a prefix to make sure table name not started with number) as the temp table name.
        QualifiedName target = QualifiedName.of(SystemSessionProperties.getDownloadRewriteDbName(session), RESULT_TABLE_NAME_PREFIX + session.getQueryId().getId());

        List<Property> properties = new ArrayList<>();
        // store the data in RCBINARY format, as it can achieve the best write performance.
        // note: CSV format only support varchar type.
        Property format = new Property(new Identifier("format", false), new StringLiteral("RCBINARY"));
        properties.add(format);

        // this is a hack.
        // make sure it can write result as fast as possible.
        session.addSystemProperty(SystemSessionProperties.SCALE_WRITERS, "false");
        session.addSystemProperty(SystemSessionProperties.REDISTRIBUTE_WRITES, "false");

        return new CreateTableAsSelect(
                target,
                query,
                false, // fail if the table already exists
                properties,
                true,
                Optional.empty(),
                COMMENT);
    }

    // add limit if not exists
    private Optional<Node> updateLimitNode(Optional<Node> limit, Session session) {
        if (!limit.isPresent()) {
            // add limit node
            Long limitNum = SystemSessionProperties.getDownloadRewriteRowLimit(session);
            if (limitNum < 1) {
                throw new IllegalArgumentException("Illegal limit: " + limit + " please make sure it's greater than 1.");
            }

            return Optional.of(new Limit(Long.toString(limitNum)));
        }

        return limit;
    }

    private QuerySpecification updateQuerySpecification(QuerySpecification querySpecification, Session session) {
        Optional<Node> updatedLimitNode = updateLimitNode(querySpecification.getLimit(), session);

        return new QuerySpecification(querySpecification.getLocation().get(),
                querySpecification.getSelect(),
                querySpecification.getFrom(),
                querySpecification.getWhere(),
                querySpecification.getGroupBy(),
                querySpecification.getHaving(),
                querySpecification.getOrderBy(),
                querySpecification.getOffset(),
                updatedLimitNode);
    }

    private Query updateQuery(Query query, Session session) {
        QueryBody queryBody = query.getQueryBody();

        QueryBody updateQueryBody = queryBody;
        Optional<Node> updatedLimitNode = query.getLimit();

        if (queryBody instanceof QuerySpecification) {
            updateQueryBody = updateQuerySpecification((QuerySpecification) queryBody, session);
        } else {
            updatedLimitNode = updateLimitNode(query.getLimit(), session);

        }

        if (updateQueryBody == null) {
            updateQueryBody = queryBody;
        }

        return new Query(
                query.getWith(),
                updateQueryBody,
                query.getOrderBy(),
                query.getOffset(),
                updatedLimitNode);
    }
}