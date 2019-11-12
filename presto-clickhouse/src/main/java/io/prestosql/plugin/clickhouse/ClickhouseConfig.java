package io.prestosql.plugin.clickhouse;

import io.airlift.configuration.Config;
import io.airlift.units.Duration;

import javax.validation.constraints.Min;
import java.util.concurrent.TimeUnit;

public class ClickhouseConfig {
    private boolean autoReconnect = true;
    private int maxReconnects = 3;
    private Duration connectionTimeout = new Duration(10, TimeUnit.SECONDS);
    private int enableOptimizePredicateExpression = 0;

    public boolean isAutoReconnect()
    {
        return autoReconnect;
    }

    @Config("clickhouse.auto-reconnect")
    public ClickhouseConfig setAutoReconnect(boolean autoReconnect)
    {
        this.autoReconnect = autoReconnect;
        return this;
    }

    @Min(1)
    public int getMaxReconnects()
    {
        return maxReconnects;
    }

    @Config("clickhouse.max-reconnects")
    public ClickhouseConfig setMaxReconnects(int maxReconnects)
    {
        this.maxReconnects = maxReconnects;
        return this;
    }

    public Duration getConnectionTimeout()
    {
        return connectionTimeout;
    }

    @Config("clickhouse.connection-timeout")
    public ClickhouseConfig setConnectionTimeout(Duration connectionTimeout)
    {
        this.connectionTimeout = connectionTimeout;
        return this;
    }

    @Config("enable_optimize_predicate_expression")
    public ClickhouseConfig setEnableOptimizePredicateExpression(int enableOptimizePredicateExpression)
    {
        this.enableOptimizePredicateExpression = enableOptimizePredicateExpression;
        return this;
    }
}
