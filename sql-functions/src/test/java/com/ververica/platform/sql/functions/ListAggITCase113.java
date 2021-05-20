package com.ververica.platform.sql.functions;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertThat;

import java.util.List;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.runtime.state.storage.JobManagerCheckpointStorage;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.Before;
import org.junit.Test;

/** Integration test for the built-in <tt>LISTAGG</tt> function. */
public class ListAggITCase113 extends AbstractTableTestBase {

  protected StreamExecutionEnvironment env;
  protected StreamTableEnvironment tEnv;

  @Before
  public void setUp() {
    env = StreamExecutionEnvironment.getExecutionEnvironment();
    // configure environment as needed
    env.setParallelism(4);
    env.getConfig().setRestartStrategy(RestartStrategies.noRestart());
    env.setStateBackend(new EmbeddedRocksDBStateBackend());
    env.getCheckpointConfig().setCheckpointStorage(new JobManagerCheckpointStorage());

    // create table environment
    tEnv =
        StreamTableEnvironment.create(
            env, EnvironmentSettings.newInstance().inStreamingMode().build());

    // initialize and register any UDFs you need, e.g.
    // tEnv.createTemporaryFunction("MyUDF", MyUDF.class);
  }

  private void createSource(Row... inputData) {
    DataStreamSource<Row> changelogStream = env.fromElements(inputData);
    tEnv.createTemporaryView("input", tEnv.fromChangelogStream(changelogStream).as("name", "age"));
  }

  private List<Row> getResult() throws Exception {
    Table resultTable = tEnv.sqlQuery("SELECT age, LISTAGG(DISTINCT name) FROM input GROUP BY age");

    DataStream<Row> resultStream = tEnv.toChangelogStream(resultTable);
    return getRowsFromDataStream(resultStream);
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
