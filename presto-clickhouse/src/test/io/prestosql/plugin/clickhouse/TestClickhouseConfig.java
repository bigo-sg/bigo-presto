package io.prestosql.plugin.clickhouse;

import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestClickhouseConfig {
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(ClickhouseConfig.class)
                .setAutoReconnect(true)
                .setMaxReconnects(3)
                .setConnectionTimeout(new Duration(10, TimeUnit.SECONDS)));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("clickhouse.auto-reconnect", "false")
                .put("clickhouse.max-reconnects", "4")
                .put("clickhouse.connection-timeout", "4s").build();

        ClickhouseConfig expected = new ClickhouseConfig()
                .setAutoReconnect(false)
                .setMaxReconnects(4)
                .setConnectionTimeout(new Duration(4, TimeUnit.SECONDS));

        assertFullMapping(properties, expected);
    }
}
