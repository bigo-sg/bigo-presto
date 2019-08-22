package io.prestosql.execution;

import io.prestosql.Session;
import io.prestosql.SystemSessionProperties;
import io.prestosql.spi.PrestoException;

import java.util.Optional;

import static io.prestosql.spi.StandardErrorCode.EXCEEDED_CPU_LIMIT;

public class SkewTaskDetector {
    static boolean shouldCheck(Session session) {
        return SystemSessionProperties.isEnforceSkewTaskLimit(session);
    }

    static Optional<PrestoException> check(SqlQueryExecution query) {
        Optional<StageInfo> stageInfo = query.getStageInfo();

        if (stageInfo.isPresent()) {

        }

        return Optional.empty();
    }

    static class ExceededSkewTaskLimitException
            extends PrestoException
    {
        ExceededSkewTaskLimitException()
        {
            super(EXCEEDED_CPU_LIMIT, "Exceeded skew task limit of ");
        }
    }
}

