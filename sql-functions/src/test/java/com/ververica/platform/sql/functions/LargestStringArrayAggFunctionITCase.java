package com.ververica.platform.sql.functions;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

import java.util.Arrays;
import java.util.List;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.Before;
import org.junit.Test;

/** Integration test for {@link LargestStringArrayAggFunction}. */
public class LargestStringArrayAggFunctionITCase extends AbstractTableTestBase {

  protected StreamExecutionEnvironment env;
  protected StreamTableEnvironment tEnv;

  @Before
  public void setUp() {
    TestValuesTableFactory.clearAllData();
    env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(4);
    tEnv =
        StreamTableEnvironment.create(
            env, EnvironmentSettings.newInstance().inStreamingMode().build());
    env.getConfig().setRestartStrategy(RestartStrategies.noRestart());
    env.setStateBackend(new RocksDBStateBackend((StateBackend) new MemoryStateBackend()));

    tEnv.createTemporaryFunction(
        "LargestStringArrayAggFunction", LargestStringArrayAggFunction.class);
  }

  private void createSource(Row... inputData) {
    final String createSource =
        String.format(
            "CREATE TABLE input ( \n"
                + "  `name` STRING,\n"
                + "  `aliases` ARRAY<STRING>"
                + ") WITH (\n"
                + "  'connector' = 'values',\n"
                + "  'data-id' = '%s',\n"
                + "  'changelog-mode' = 'I,UA,UB,D'\n"
                + ")",
            TestValuesTableFactory.registerData(Arrays.asList(inputData)));
    tEnv.executeSql(createSource);
  }

  private List<Row> executeSql() throws Exception {
    TableResult resultTable =
        tEnv.executeSql(
            "SELECT\n"
                + "  name,\n"
                + "  LargestStringArrayAggFunction(aliases)\n"
                + "FROM input\n"
                + "GROUP BY name");
    return getRowsFromTable(resultTable);
  }

  @Test
  public void aggregation1() throws Exception {
    createSource(
        Row.ofKind(RowKind.INSERT, "john", new String[] {"john@test.com"}),
        Row.ofKind(RowKind.INSERT, "john", new String[] {"john@test.com", "john@apache.org"}),
        Row.ofKind(RowKind.INSERT, "john", new String[] {"john@test2.com"}),
        Row.ofKind(
            RowKind.INSERT,
            "john",
            new String[] {"john@test.com", "john@apache.org", "john@mail.ru"}));

    assertThat(
        executeSql(),
        containsInAnyOrder(
            Row.ofKind(RowKind.INSERT, "john", new String[] {"john@test.com"}),
            Row.ofKind(RowKind.UPDATE_BEFORE, "john", new String[] {"john@test.com"}),
            Row.ofKind(
                RowKind.UPDATE_AFTER, "john", new String[] {"john@test.com", "john@apache.org"}),
            Row.ofKind(
                RowKind.UPDATE_BEFORE, "john", new String[] {"john@test.com", "john@apache.org"}),
            Row.ofKind(
                RowKind.UPDATE_AFTER,
                "john",
                new String[] {"john@test.com", "john@apache.org", "john@mail.ru"})));
  }

  @Test
  public void aggregation2() throws Exception {
    createSource(
        Row.ofKind(RowKind.INSERT, "john", new String[] {"john@test.com"}),
        Row.ofKind(RowKind.UPDATE_BEFORE, "john", new String[] {"john@test.com"}),
        Row.ofKind(RowKind.UPDATE_AFTER, "john", new String[] {"john@test.com", "john@apache.org"}),
        Row.ofKind(
            RowKind.UPDATE_BEFORE, "john", new String[] {"john@test.com", "john@apache.org"}),
        Row.ofKind(RowKind.UPDATE_AFTER, "john", new String[] {"john@test2.com"}),
        Row.ofKind(RowKind.UPDATE_BEFORE, "john", new String[] {"john@test2.com"}),
        Row.ofKind(
            RowKind.UPDATE_AFTER,
            "john",
            new String[] {"john@test.com", "john@apache.org", "john@mail.ru"}));

    assertThat(
        executeSql(),
        containsInAnyOrder(
            Row.ofKind(RowKind.INSERT, "john", new String[] {"john@test.com"}),
            Row.ofKind(RowKind.DELETE, "john", new String[] {"john@test.com"}),
            Row.ofKind(RowKind.INSERT, "john", new String[] {"john@test.com", "john@apache.org"}),
            Row.ofKind(RowKind.DELETE, "john", new String[] {"john@test.com", "john@apache.org"}),
            Row.ofKind(RowKind.INSERT, "john", new String[] {"john@test2.com"}),
            Row.ofKind(RowKind.DELETE, "john", new String[] {"john@test2.com"}),
            Row.ofKind(
                RowKind.INSERT,
                "john",
                new String[] {"john@test.com", "john@apache.org", "john@mail.ru"})));
  }
}
