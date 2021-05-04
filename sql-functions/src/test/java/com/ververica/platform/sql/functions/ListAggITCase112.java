package com.ververica.platform.sql.functions;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.Before;
import org.junit.Test;

/** Integration test for the built-in <tt>LISTAGG</tt> function. */
public class ListAggITCase112 {

  protected StreamExecutionEnvironment env;
  protected StreamTableEnvironment tEnv;

  @Before
  public void setUp() {
    env = StreamExecutionEnvironment.getExecutionEnvironment();
    // configure environment as needed
    env.setParallelism(4);
    env.getConfig().setRestartStrategy(RestartStrategies.noRestart());
    env.setStateBackend(new RocksDBStateBackend((StateBackend) new MemoryStateBackend()));

    // create table environment
    tEnv =
        StreamTableEnvironment.create(
            env, EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build());
    TestValuesTableFactory.clearAllData();

    // initialize and register any UDFs you need, e.g.
    // tEnv.createTemporaryFunction("MyUDF", MyUDF.class);
  }

  private void createSource(Row... inputData) {
    final String createSource =
        String.format(
            "CREATE TABLE input ( \n"
                + "  `name` STRING,\n"
                + "  `age` INT"
                + ") WITH (\n"
                + "  'connector' = 'values',\n"
                + "  'data-id' = '%s',\n"
                + "  'changelog-mode' = 'I,UA,UB,D'\n"
                + ")",
            TestValuesTableFactory.registerData(Arrays.asList(inputData)));
    tEnv.executeSql(createSource);
  }

  private List<String> getResult() throws InterruptedException, ExecutionException {
    tEnv.executeSql(
        "CREATE TABLE sink (\n"
            + "    age INT,\n"
            + "    names String\n"
            + ") WITH (\n"
            + "  'connector' = 'values',\n"
            + "  'sink-insert-only' = 'false',\n"
            + "  'changelog-mode' = 'I,UA,UB,D'\n"
            + ")");

    tEnv.executeSql(
            "INSERT INTO sink\n"
                + "SELECT\n"
                + "  age,\n"
                + "  LISTAGG(DISTINCT name)\n"
                + "FROM input\n"
                + "GROUP BY age")
        .await();

    return TestValuesTableFactory.getRawResults("sink");
  }

  @Test
  public void testListAgg1() throws ExecutionException, InterruptedException {
    createSource(
        Row.ofKind(RowKind.INSERT, "john", 32),
        Row.ofKind(RowKind.INSERT, "john", 32),
        Row.ofKind(RowKind.UPDATE_BEFORE, "john", 32),
        Row.ofKind(RowKind.UPDATE_AFTER, "john", 33));

    List<String> actual = getResult();

    List<String> expected = Arrays.asList("+I(32,john)", "+I(33,john)");

    actual.sort(Comparator.naturalOrder());
    expected.sort(Comparator.naturalOrder());
    assertEquals(expected, actual);
  }

  @Test
  public void testListAgg2() throws ExecutionException, InterruptedException {
    createSource(
        Row.ofKind(RowKind.INSERT, "john", 32),
        Row.ofKind(RowKind.UPDATE_BEFORE, "john", 32),
        Row.ofKind(RowKind.UPDATE_AFTER, "john", 33));

    List<String> actual = getResult();

    List<String> expected = Arrays.asList("+I(32,john)", "-D(32,john)", "+I(33,john)");

    actual.sort(Comparator.naturalOrder());
    expected.sort(Comparator.naturalOrder());
    assertEquals(expected, actual);
  }

  @Test
  public void testListAgg3() throws Exception {
    createSource(Row.ofKind(RowKind.INSERT, "john", 32), Row.ofKind(RowKind.INSERT, "alice", 32));

    List<String> actual = getResult();

    List<String> expected = Arrays.asList("+I(32,john)", "-U(32,john)", "+U(32,john,alice)");

    actual.sort(Comparator.naturalOrder());
    expected.sort(Comparator.naturalOrder());
    assertEquals(expected, actual);
  }
}
