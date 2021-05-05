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
import org.apache.flink.table.functions.UserDefinedFunction;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/** Integration test for {@link ArrayListAggFunction} and {@link ArrayListAggFunction2}. */
@RunWith(Parameterized.class)
public class ArrayListAggFunctionITCase extends AbstractTableTestBase {

  protected StreamExecutionEnvironment env;
  protected StreamTableEnvironment tEnv;

  @Parameterized.Parameter public Class<UserDefinedFunction> implementation;

  @Parameterized.Parameters(name = "implementation = {0}")
  public static Iterable<Class<? extends UserDefinedFunction>> parameters() {
    return Arrays.asList(ArrayListAggFunction.class, ArrayListAggFunction2.class);
  }

  @Before
  public void setUp() {
    TestValuesTableFactory.clearAllData();
    env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(4);
    tEnv =
        StreamTableEnvironment.create(
            env, EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build());
    env.getConfig().setRestartStrategy(RestartStrategies.noRestart());
    env.setStateBackend(new RocksDBStateBackend((StateBackend) new MemoryStateBackend()));

    tEnv.createTemporaryFunction("ArrayListAggFunction", implementation);
  }

  private void createSource(Row... inputData) {
    final String createSource =
        String.format(
            "CREATE TABLE input ( \n"
                + "  `name` STRING,\n"
                + "  `age` INT"
                + ") WITH (\n"
                + "  'connector' = 'values',\n"
                + "  'data-id' = '%s'\n"
                + ")",
            TestValuesTableFactory.registerData(Arrays.asList(inputData)));
    tEnv.executeSql(createSource);
  }

  private List<Row> executeSql() throws Exception {
    TableResult resultTable =
        tEnv.executeSql(
            "SELECT\n"
                + "  age,\n"
                + "  ArrayListAggFunction(DISTINCT name)\n"
                + "FROM input\n"
                + "GROUP BY age");
    return getRowsFromTable(resultTable);
  }

  @Test
  public void aggregation1() throws Exception {
    createSource(Row.of("john", 35), Row.of("alice", 32), Row.of("bob", 35), Row.of("sarah", 32));

    assertThat(
        executeSql(),
        containsInAnyOrder(
            Row.ofKind(RowKind.INSERT, 35, new String[] {"john"}),
            Row.ofKind(RowKind.UPDATE_BEFORE, 35, new String[] {"john"}),
            Row.ofKind(RowKind.UPDATE_AFTER, 35, new String[] {"john", "bob"}),
            Row.ofKind(RowKind.INSERT, 32, new String[] {"alice"}),
            Row.ofKind(RowKind.UPDATE_BEFORE, 32, new String[] {"alice"}),
            Row.ofKind(RowKind.UPDATE_AFTER, 32, new String[] {"alice", "sarah"})));
  }
}
