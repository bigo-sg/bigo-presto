package io.prestosql.execution;

import com.google.common.collect.ImmutableList;
import io.prestosql.sql.parser.ParsingOptions;
import io.prestosql.sql.parser.SqlParser;
import io.prestosql.sql.parser.hive.LoadData;
import javafx.util.Pair;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.List;

/**
 * @author tangyun@bigo.sg
 * @date 1/7/20 5:09 PM
 */
public class TestLoadDataTask {

    private static SqlParser sqlParser = new SqlParser();
    private static ParsingOptions hiveParsingOptions = new ParsingOptions(ParsingOptions.DecimalLiteralTreatment.AS_DECIMAL);

    static {
        hiveParsingOptions.setIfUseHiveParser(true);
    }

    @Test
    public void getPartitionEnd()
    {
        List<Pair<String, String>> pairs = ImmutableList.of(
                new Pair<>("day", "2019-12-12"),
                new Pair<>("hour", "00"),
                new Pair<>("event_id", "00001")
        );

        Assert.assertEquals(LoadDataTask.getPartitionEnd(pairs), "/day=2019-12-12/hour=00/event_id=00001/");
    }

    @Test
    public void getPartitions()
    {
        List<Pair<String, String>> pairs = ImmutableList.of(
                new Pair<>("day", "2019-12-12"),
                new Pair<>("hour", "00"),
                new Pair<>("event_id", "00001")
        );

        String sql = "load data inpath 'path' into table tbl partition(day='2019-12-12'," +
                "event_id='00001', hour='00')";
        LoadData loadData = (LoadData) sqlParser.createStatement(sql, hiveParsingOptions);
        List<String> tabPartitions = ImmutableList.of(
                "day",
                "hour",
                "event_id"
        );

        Assert.assertEquals(pairs,
                LoadDataTask.getPartitions(loadData.getPartitions(), tabPartitions, loadData));
    }
}
