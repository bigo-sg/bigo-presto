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

        Query query = (Query) node;

        // check limit
        QuerySpecification querySpecification = (QuerySpecification) query.getQueryBody();
        Optional<Node> limitNode = querySpecification.getLimit();
        if (!limitNode.isPresent()) {
            // add limit node
            Long limit = SystemSessionProperties.getDownloadRewriteRowLimit(session);
            if (limit < 1) {
                throw new IllegalArgumentException("Illegal limit: " + limit + " please make sure it's greater than 1.");
            }

            Optional<Node> generatedLimitNode = Optional.of(new Limit(Long.toString(limit)));

            QuerySpecification querySpecificationWithLimit = new QuerySpecification(querySpecification.getLocation().get(),
                    querySpecification.getSelect(),
                    querySpecification.getFrom(),
                    querySpecification.getWhere(),
                    querySpecification.getGroupBy(),
                    querySpecification.getHaving(),
                    querySpecification.getOrderBy(),
                    querySpecification.getOffset(),
                    generatedLimitNode);

            query = new Query(
                    query.getWith(),
                    querySpecificationWithLimit,
                    query.getOrderBy(),
                    query.getOffset(),
                    query.getLimit());
        }

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
}
