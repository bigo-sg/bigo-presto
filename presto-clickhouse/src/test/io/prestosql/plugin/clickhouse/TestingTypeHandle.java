package io.prestosql.plugin.clickhouse;

import io.prestosql.plugin.jdbc.JdbcTypeHandle;

import java.sql.Types;
import java.util.Optional;

public final class TestingTypeHandle
{
    private TestingTypeHandle() {}

    public static final JdbcTypeHandle JDBC_BOOLEAN = new JdbcTypeHandle(Types.BOOLEAN, Optional.of("boolean"), 1, 0, Optional.empty());

    public static final JdbcTypeHandle JDBC_SMALLINT = new JdbcTypeHandle(Types.SMALLINT, Optional.of("smallint"), 1, 0, Optional.empty());
    public static final JdbcTypeHandle JDBC_TINYINT = new JdbcTypeHandle(Types.TINYINT, Optional.of("tinyint"), 2, 0, Optional.empty());
    public static final JdbcTypeHandle JDBC_INTEGER = new JdbcTypeHandle(Types.INTEGER, Optional.of("integer"), 4, 0, Optional.empty());
    public static final JdbcTypeHandle JDBC_BIGINT = new JdbcTypeHandle(Types.BIGINT, Optional.of("bigint"), 8, 0, Optional.empty());

    public static final JdbcTypeHandle JDBC_REAL = new JdbcTypeHandle(Types.REAL, Optional.of("real"), 8, 0, Optional.empty());
    public static final JdbcTypeHandle JDBC_DOUBLE = new JdbcTypeHandle(Types.DOUBLE, Optional.of("double precision"), 8, 0, Optional.empty());

    public static final JdbcTypeHandle JDBC_CHAR = new JdbcTypeHandle(Types.CHAR, Optional.of("char"), 10, 0, Optional.empty());
    public static final JdbcTypeHandle JDBC_VARCHAR = new JdbcTypeHandle(Types.VARCHAR, Optional.of("varchar"), 10, 0, Optional.empty());

    public static final JdbcTypeHandle JDBC_DATE = new JdbcTypeHandle(Types.DATE, Optional.of("date"), 8, 0, Optional.empty());
    public static final JdbcTypeHandle JDBC_TIME = new JdbcTypeHandle(Types.TIME, Optional.of("time"), 4, 0, Optional.empty());
    public static final JdbcTypeHandle JDBC_TIMESTAMP = new JdbcTypeHandle(Types.TIMESTAMP, Optional.of("timestamp"), 8, 0, Optional.empty());
}
