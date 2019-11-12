package io.prestosql.plugin.clickhouse;

import com.google.common.collect.ImmutableMap;
import io.prestosql.spi.Plugin;
import io.prestosql.spi.connector.ConnectorFactory;
import io.prestosql.testing.TestingConnectorContext;
import org.testng.annotations.Test;

import static com.google.common.collect.Iterables.getOnlyElement;

public class TestClickhousePlugin
{
    @Test
    public void testCreateConnector()
    {
        Plugin plugin = new ClickhousePlugin();
        ConnectorFactory factory = getOnlyElement(plugin.getConnectorFactories());
        factory.create("test", ImmutableMap.of("connection-url", "jdbc:clickhouse://test:test@ip:8123"), new TestingConnectorContext());
    }
}
