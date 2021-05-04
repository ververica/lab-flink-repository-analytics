package com.ververica.platform.sql.functions;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertThat;

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
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.CloseableIterator;
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
    // can use this instead if there are only inserts and no need for row times, watermarks,...:
    // tEnv.createTemporaryView("input", tEnv.fromValues((Object[]) inputData));

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

  private List<Row> getResult() throws Exception {
    Table resultTable = tEnv.sqlQuery("SELECT age, LISTAGG(DISTINCT name) FROM input GROUP BY age");

    try (CloseableIterator<Row> rowCloseableIterator = resultTable.execute().collect()) {
      List<Row> results = new ArrayList<>();
      rowCloseableIterator.forEachRemaining(results::add);
      return results;
    }
  }

  @Test
  public void testListAgg1() throws Exception {
    createSource(
        Row.ofKind(RowKind.INSERT, "john", 32),
        Row.ofKind(RowKind.INSERT, "john", 32),
        Row.ofKind(RowKind.UPDATE_BEFORE, "john", 32),
        Row.ofKind(RowKind.UPDATE_AFTER, "john", 33));

    assertThat(
        getResult(),
        containsInAnyOrder(
            Row.ofKind(RowKind.INSERT, 32, "john"), Row.ofKind(RowKind.INSERT, 33, "john")));
  }

  @Test
  public void testListAgg2() throws Exception {
    createSource(
        Row.ofKind(RowKind.INSERT, "john", 32),
        Row.ofKind(RowKind.UPDATE_BEFORE, "john", 32),
        Row.ofKind(RowKind.UPDATE_AFTER, "john", 33));

    assertThat(
        getResult(),
        containsInAnyOrder(
            Row.ofKind(RowKind.INSERT, 32, "john"),
            Row.ofKind(RowKind.DELETE, 32, "john"),
            Row.ofKind(RowKind.INSERT, 33, "john")));
  }

  @Test
  public void testListAgg3() throws Exception {
    createSource(Row.ofKind(RowKind.INSERT, "john", 32), Row.ofKind(RowKind.INSERT, "alice", 32));

    assertThat(
        getResult(),
        containsInAnyOrder(
            Row.ofKind(RowKind.INSERT, 32, "john"),
            Row.ofKind(RowKind.UPDATE_BEFORE, 32, "john"),
            Row.ofKind(RowKind.UPDATE_AFTER, 32, "john,alice")));
  }
}
