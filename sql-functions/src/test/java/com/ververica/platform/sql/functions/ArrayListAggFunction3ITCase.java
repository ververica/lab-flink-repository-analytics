package com.ververica.platform.sql.functions;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.CloseableIterator;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/** Integration test for {@link ArrayListAggFunction3} and {@link ArrayListAggFunction4}. */
@RunWith(Parameterized.class)
public class ArrayListAggFunction3ITCase {

  protected StreamExecutionEnvironment env;
  protected StreamTableEnvironment tEnv;

  @Parameterized.Parameter public Class<UserDefinedFunction> implementation;

  @Parameterized.Parameters(name = "implementation = {0}")
  public static Iterable<Class<? extends UserDefinedFunction>> parameters() {
    return Arrays.asList(ArrayListAggFunction4.class, ArrayListAggFunction3.class);
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

  /* TODO: rewrite (all) tests to use new fromChangelogStream/toChangelogStream, e.g. as in
   * DataStreamJavaITCase.testFromAndToChangelogStreamUpsert()
   */

  private void createSource(String elementType, Row... inputData) {
    final String createSource =
        String.format(
            "CREATE TABLE input ( \n"
                + "  `name` %s,\n"
                + "  `age` INT"
                + ") WITH (\n"
                + "  'connector' = 'values',\n"
                + "  'data-id' = '%s'\n"
                + ")",
            elementType, TestValuesTableFactory.registerData(Arrays.asList(inputData)));
    tEnv.executeSql(createSource);
  }

  private List<Row> executeSql() throws Exception {
    Table resultTable =
        tEnv.sqlQuery(
            "SELECT\n"
                + "  age,\n"
                + "  ArrayListAggFunction(DISTINCT name)\n"
                + "FROM input\n"
                + "GROUP BY age");

    try (CloseableIterator<Row> rowCloseableIterator = resultTable.execute().collect()) {
      List<Row> results = new ArrayList<>();
      rowCloseableIterator.forEachRemaining(results::add);
      return results;
    }
  }

  @Test
  public void stringAggregation1() throws Exception {
    String elementType = "STRING";
    createSource(
        elementType,
        Row.of("john", 35),
        Row.of("alice", 32),
        Row.of("bob", 35),
        Row.of("sarah", 32));

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

  @Test
  public void intAggregation1() throws Exception {
    String elementType = "INT";
    createSource(elementType, Row.of(1, 35), Row.of(11, 32), Row.of(2, 35), Row.of(12, 32));

    assertThat(
        executeSql(),
        containsInAnyOrder(
            Row.ofKind(RowKind.INSERT, 35, new Integer[] {1}),
            Row.ofKind(RowKind.UPDATE_BEFORE, 35, new Integer[] {1}),
            Row.ofKind(RowKind.UPDATE_AFTER, 35, new Integer[] {1, 2}),
            Row.ofKind(RowKind.INSERT, 32, new Integer[] {11}),
            Row.ofKind(RowKind.UPDATE_BEFORE, 32, new Integer[] {11}),
            Row.ofKind(RowKind.UPDATE_AFTER, 32, new Integer[] {11, 12})));
  }
}
