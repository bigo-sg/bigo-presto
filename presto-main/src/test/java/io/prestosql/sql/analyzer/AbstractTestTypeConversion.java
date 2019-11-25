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
package io.prestosql.sql.analyzer;

import com.google.common.collect.ImmutableList;
import io.prestosql.Session;
import io.prestosql.SystemSessionProperties;
import io.prestosql.connector.CatalogName;
import io.prestosql.connector.informationschema.InformationSchemaConnector;
import io.prestosql.connector.system.SystemConnector;
import io.prestosql.execution.warnings.WarningCollector;
import io.prestosql.metadata.Catalog;
import io.prestosql.metadata.CatalogManager;
import io.prestosql.metadata.InMemoryNodeManager;
import io.prestosql.metadata.InternalNodeManager;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.QualifiedObjectName;
import io.prestosql.operator.scalar.FunctionAssertions;
import io.prestosql.security.AccessControl;
import io.prestosql.security.AccessControlManager;
import io.prestosql.security.AllowAllAccessControl;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.Connector;
import io.prestosql.spi.connector.ConnectorMetadata;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.connector.ConnectorViewDefinition;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.security.BasicPrincipal;
import io.prestosql.spi.security.Identity;
import io.prestosql.spi.session.PropertyMetadata;
import io.prestosql.spi.transaction.IsolationLevel;
import io.prestosql.spi.type.ArrayType;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.parser.SqlParser;
import io.prestosql.sql.tree.Statement;
import io.prestosql.testing.TestingMetadata;
import io.prestosql.testing.assertions.PrestoExceptionAssert;
import io.prestosql.transaction.TransactionManager;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.BeforeClass;

import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

import static io.prestosql.connector.CatalogName.createInformationSchemaCatalogName;
import static io.prestosql.connector.CatalogName.createSystemTablesCatalogName;
import static io.prestosql.metadata.MetadataManager.createTestMetadataManager;
import static io.prestosql.operator.scalar.ApplyFunction.APPLY_FUNCTION;
import static io.prestosql.spi.session.PropertyMetadata.integerProperty;
import static io.prestosql.spi.session.PropertyMetadata.stringProperty;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.testing.TestingSession.testSessionBuilder;
import static io.prestosql.testing.assertions.PrestoExceptionAssert.assertPrestoExceptionThrownBy;
import static io.prestosql.transaction.InMemoryTransactionManager.createTestTransactionManager;
import static io.prestosql.transaction.TransactionBuilder.transaction;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;

public abstract class AbstractTestTypeConversion {
    protected FunctionAssertions functionAssertions;

    private static final String TPCH_CATALOG = "tpch";
    private static final CatalogName TPCH_CATALOG_NAME = new CatalogName(TPCH_CATALOG);
    private static final String SECOND_CATALOG = "c2";
    private static final CatalogName SECOND_CATALOG_NAME = new CatalogName(SECOND_CATALOG);
    private static final String THIRD_CATALOG = "c3";
    private static final CatalogName THIRD_CATALOG_NAME = new CatalogName(THIRD_CATALOG);
    private static final String CATALOG_FOR_IDENTIFIER_CHAIN_TESTS = "cat";
    private static final CatalogName CATALOG_FOR_IDENTIFIER_CHAIN_TESTS_NAME = new CatalogName(CATALOG_FOR_IDENTIFIER_CHAIN_TESTS);
    private static final Session SETUP_SESSION = testSessionBuilder()
            .setCatalog("c1")
            .setSchema("s1")
            .build();

    private static final Session CLIENT_SESSION_HIVE = testSessionBuilder()
            .setCatalog(TPCH_CATALOG)
            .setSchema("s1")
            .setIdentity(Identity.forUser("test").withPrincipal(new BasicPrincipal("principal")).build())
            .setSystemProperty(SystemSessionProperties.ENABLE_HIVE_SQL_SYNTAX, "true")
            .build();

    private static final Session CLIENT_SESSION_PRESTO = testSessionBuilder()
            .setCatalog(TPCH_CATALOG)
            .setSchema("s1")
            .setIdentity(Identity.forUser("test").withPrincipal(new BasicPrincipal("principal")).build())
            .build();

    private static final SqlParser SQL_PARSER = new SqlParser();

    private TransactionManager transactionManager;
    private AccessControl accessControl;
    private Metadata metadata;

    private Catalog createTestingCatalog(String catalogName, CatalogName catalog)
    {
        CatalogName systemId = createSystemTablesCatalogName(catalog);
        Connector connector = createTestingConnector();
        InternalNodeManager nodeManager = new InMemoryNodeManager();
        return new Catalog(
                catalogName,
                catalog,
                connector,
                createInformationSchemaCatalogName(catalog),
                new InformationSchemaConnector(catalogName, nodeManager, metadata, accessControl),
                systemId,
                new SystemConnector(
                        nodeManager,
                        connector.getSystemTables(),
                        transactionId -> transactionManager.getConnectorTransaction(transactionId, catalog)));
    }

    private void inSetupTransaction(Consumer<Session> consumer)
    {
        transaction(transactionManager, accessControl)
                .singleStatement()
                .readUncommitted()
                .execute(SETUP_SESSION, consumer);
    }

    @BeforeClass
    public void setup()
    {
        CatalogManager catalogManager = new CatalogManager();
        transactionManager = createTestTransactionManager(catalogManager);
        accessControl = new AccessControlManager(transactionManager);

        metadata = createTestMetadataManager(transactionManager, new FeaturesConfig());
        metadata.addFunctions(ImmutableList.of(APPLY_FUNCTION));

        Catalog tpchTestCatalog = createTestingCatalog(TPCH_CATALOG, TPCH_CATALOG_NAME);
        catalogManager.registerCatalog(tpchTestCatalog);
        metadata.getTablePropertyManager().addProperties(TPCH_CATALOG_NAME, tpchTestCatalog.getConnector(TPCH_CATALOG_NAME).getTableProperties());
        metadata.getAnalyzePropertyManager().addProperties(TPCH_CATALOG_NAME, tpchTestCatalog.getConnector(TPCH_CATALOG_NAME).getAnalyzeProperties());

        catalogManager.registerCatalog(createTestingCatalog(SECOND_CATALOG, SECOND_CATALOG_NAME));
        catalogManager.registerCatalog(createTestingCatalog(THIRD_CATALOG, THIRD_CATALOG_NAME));

        SchemaTableName table1 = new SchemaTableName("s1", "t1");
        inSetupTransaction(session -> metadata.createTable(session, TPCH_CATALOG,
                new ConnectorTableMetadata(table1, ImmutableList.of(
                        new ColumnMetadata("a", BIGINT),
                        new ColumnMetadata("b", BIGINT),
                        new ColumnMetadata("c", BIGINT),
                        new ColumnMetadata("d", BIGINT))),
                false));

        SchemaTableName table2 = new SchemaTableName("s1", "t2");
        inSetupTransaction(session -> metadata.createTable(session, TPCH_CATALOG,
                new ConnectorTableMetadata(table2, ImmutableList.of(
                        new ColumnMetadata("a", BIGINT),
                        new ColumnMetadata("b", BIGINT))),
                false));

        SchemaTableName table3 = new SchemaTableName("s1", "t3");
        inSetupTransaction(session -> metadata.createTable(session, TPCH_CATALOG,
                new ConnectorTableMetadata(table3, ImmutableList.of(
                        new ColumnMetadata("a", BIGINT),
                        new ColumnMetadata("b", BIGINT),
                        new ColumnMetadata("x", BIGINT, null, true))),
                false));

        // table in different catalog
        SchemaTableName table4 = new SchemaTableName("s2", "t4");
        inSetupTransaction(session -> metadata.createTable(session, SECOND_CATALOG,
                new ConnectorTableMetadata(table4, ImmutableList.of(
                        new ColumnMetadata("a", BIGINT))),
                false));

        // table with a hidden column
        SchemaTableName table5 = new SchemaTableName("s1", "t5");
        inSetupTransaction(session -> metadata.createTable(session, TPCH_CATALOG,
                new ConnectorTableMetadata(table5, ImmutableList.of(
                        new ColumnMetadata("a", BIGINT),
                        new ColumnMetadata("b", BIGINT, null, true))),
                false));

        // table with a varchar column
        SchemaTableName table6 = new SchemaTableName("s1", "t6");
        inSetupTransaction(session -> metadata.createTable(session, TPCH_CATALOG,
                new ConnectorTableMetadata(table6, ImmutableList.of(
                        new ColumnMetadata("a", BIGINT),
                        new ColumnMetadata("b", VARCHAR),
                        new ColumnMetadata("c", BIGINT),
                        new ColumnMetadata("d", BIGINT))),
                false));

        // table with bigint, double, array of bigints and array of doubles column
        SchemaTableName table7 = new SchemaTableName("s1", "t7");
        inSetupTransaction(session -> metadata.createTable(session, TPCH_CATALOG,
                new ConnectorTableMetadata(table7, ImmutableList.of(
                        new ColumnMetadata("a", BIGINT),
                        new ColumnMetadata("b", DOUBLE),
                        new ColumnMetadata("c", new ArrayType(BIGINT)),
                        new ColumnMetadata("d", new ArrayType(DOUBLE)))),
                false));

        // valid view referencing table in same schema
        ConnectorViewDefinition viewData1 = new ConnectorViewDefinition(
                "select a from t1",
                Optional.of(TPCH_CATALOG),
                Optional.of("s1"),
                ImmutableList.of(new ConnectorViewDefinition.ViewColumn("a", BIGINT.getTypeId())),
                Optional.of("user"),
                false);
        inSetupTransaction(session -> metadata.createView(session, new QualifiedObjectName(TPCH_CATALOG, "s1", "v1"), viewData1, false));

        // stale view (different column type)
        ConnectorViewDefinition viewData2 = new ConnectorViewDefinition(
                "select a from t1",
                Optional.of(TPCH_CATALOG),
                Optional.of("s1"),
                ImmutableList.of(new ConnectorViewDefinition.ViewColumn("a", VARCHAR.getTypeId())),
                Optional.of("user"),
                false);
        inSetupTransaction(session -> metadata.createView(session, new QualifiedObjectName(TPCH_CATALOG, "s1", "v2"), viewData2, false));

        // view referencing table in different schema from itself and session
        ConnectorViewDefinition viewData3 = new ConnectorViewDefinition(
                "select a from t4",
                Optional.of(SECOND_CATALOG),
                Optional.of("s2"),
                ImmutableList.of(new ConnectorViewDefinition.ViewColumn("a", BIGINT.getTypeId())),
                Optional.of("owner"),
                false);
        inSetupTransaction(session -> metadata.createView(session, new QualifiedObjectName(THIRD_CATALOG, "s3", "v3"), viewData3, false));

        // valid view with uppercase column name
        ConnectorViewDefinition viewData4 = new ConnectorViewDefinition(
                "select A from t1",
                Optional.of(TPCH_CATALOG),
                Optional.of("s1"),
                ImmutableList.of(new ConnectorViewDefinition.ViewColumn("a", BIGINT.getTypeId())),
                Optional.of("user"),
                false);
        inSetupTransaction(session -> metadata.createView(session, new QualifiedObjectName(TPCH_CATALOG, "s1", "v4"), viewData4, false));

        // recursive view referencing to itself
        ConnectorViewDefinition viewData5 = new ConnectorViewDefinition(
                "select * from v5",
                Optional.of(TPCH_CATALOG),
                Optional.of("s1"),
                ImmutableList.of(new ConnectorViewDefinition.ViewColumn("a", BIGINT.getTypeId())),
                Optional.of("user"),
                false);
        inSetupTransaction(session -> metadata.createView(session, new QualifiedObjectName(TPCH_CATALOG, "s1", "v5"), viewData5, false));

        // for identifier chain resolving tests
        catalogManager.registerCatalog(createTestingCatalog(CATALOG_FOR_IDENTIFIER_CHAIN_TESTS, CATALOG_FOR_IDENTIFIER_CHAIN_TESTS_NAME));
        Type singleFieldRowType = metadata.fromSqlType("row(f1 bigint)");
        Type rowType = metadata.fromSqlType("row(f1 bigint, f2 bigint)");
        Type nestedRowType = metadata.fromSqlType("row(f1 row(f11 bigint, f12 bigint), f2 boolean)");
        Type doubleNestedRowType = metadata.fromSqlType("row(f1 row(f11 row(f111 bigint, f112 bigint), f12 boolean), f2 boolean)");

        SchemaTableName b = new SchemaTableName("a", "b");
        inSetupTransaction(session -> metadata.createTable(session, CATALOG_FOR_IDENTIFIER_CHAIN_TESTS,
                new ConnectorTableMetadata(b, ImmutableList.of(
                        new ColumnMetadata("x", VARCHAR))),
                false));

        SchemaTableName t1 = new SchemaTableName("a", "t1");
        inSetupTransaction(session -> metadata.createTable(session, CATALOG_FOR_IDENTIFIER_CHAIN_TESTS,
                new ConnectorTableMetadata(t1, ImmutableList.of(
                        new ColumnMetadata("b", rowType))),
                false));

        SchemaTableName t2 = new SchemaTableName("a", "t2");
        inSetupTransaction(session -> metadata.createTable(session, CATALOG_FOR_IDENTIFIER_CHAIN_TESTS,
                new ConnectorTableMetadata(t2, ImmutableList.of(
                        new ColumnMetadata("a", rowType))),
                false));

        SchemaTableName t3 = new SchemaTableName("a", "t3");
        inSetupTransaction(session -> metadata.createTable(session, CATALOG_FOR_IDENTIFIER_CHAIN_TESTS,
                new ConnectorTableMetadata(t3, ImmutableList.of(
                        new ColumnMetadata("b", nestedRowType),
                        new ColumnMetadata("c", BIGINT))),
                false));

        SchemaTableName t4 = new SchemaTableName("a", "t4");
        inSetupTransaction(session -> metadata.createTable(session, CATALOG_FOR_IDENTIFIER_CHAIN_TESTS,
                new ConnectorTableMetadata(t4, ImmutableList.of(
                        new ColumnMetadata("b", doubleNestedRowType),
                        new ColumnMetadata("c", BIGINT))),
                false));

        SchemaTableName t5 = new SchemaTableName("a", "t5");
        inSetupTransaction(session -> metadata.createTable(session, CATALOG_FOR_IDENTIFIER_CHAIN_TESTS,
                new ConnectorTableMetadata(t5, ImmutableList.of(
                        new ColumnMetadata("b", singleFieldRowType))),
                false));
    }

    protected static Analyzer createAnalyzer(Session session, Metadata metadata)
    {
        return new Analyzer(
                session,
                metadata,
                SQL_PARSER,
                new AllowAllAccessControl(),
                Optional.empty(),
                emptyList(),
                emptyMap(),
                WarningCollector.NOOP);
    }

    protected void analyzeHive(@Language("SQL") String query)
    {
        analyze(CLIENT_SESSION_HIVE, query);
    }


    protected void analyzePresto(@Language("SQL") String query)
    {
        analyze(CLIENT_SESSION_PRESTO, query);
    }

    protected void analyze(Session clientSession, @Language("SQL") String query)
    {
        transaction(transactionManager, accessControl)
                .singleStatement()
                .readUncommitted()
                .execute(clientSession, session -> {
                    Analyzer analyzer = createAnalyzer(session, metadata);
                    Statement statement = SQL_PARSER.createStatement(query);
                    analyzer.analyze(statement);
                });
    }

    protected PrestoExceptionAssert assertFails(Session session, @Language("SQL") String query)
    {
        return assertPrestoExceptionThrownBy(() -> analyze(session, query));
    }

    protected PrestoExceptionAssert assertFailsHive(@Language("SQL") String query)
    {
        return assertFails(CLIENT_SESSION_HIVE, query);
    }

    protected PrestoExceptionAssert assertFailsPresto(@Language("SQL") String query)
    {
        return assertFails(CLIENT_SESSION_PRESTO, query);
    }

    protected void assertFunction(String projection, Type expectedType, Object expected)
    {
        functionAssertions.assertFunction(projection, expectedType, expected);
    }

    private static Connector createTestingConnector()
    {
        return new Connector()
        {
            private final ConnectorMetadata metadata = new TestingMetadata();

            @Override
            public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly)
            {
                return new ConnectorTransactionHandle() {};
            }

            @Override
            public ConnectorMetadata getMetadata(ConnectorTransactionHandle transaction)
            {
                return metadata;
            }

            @Override
            public List<PropertyMetadata<?>> getAnalyzeProperties()
            {
                return ImmutableList.of(
                        stringProperty("p1", "test string property", "", false),
                        integerProperty("p2", "test integer property", 0, false));
            }
        };
    }
}
