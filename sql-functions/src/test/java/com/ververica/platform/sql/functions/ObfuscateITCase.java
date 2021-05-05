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

/** Integration test for {@link Obfuscate}. */
public class ObfuscateITCase extends AbstractTableTestBase {

  protected StreamExecutionEnvironment env;
  protected StreamTableEnvironment tEnv;

  @Before
  public void setUp() {
    env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(4);
    tEnv =
        StreamTableEnvironment.create(
            env, EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build());
    env.getConfig().setRestartStrategy(RestartStrategies.noRestart());
    env.setStateBackend(new RocksDBStateBackend((StateBackend) new MemoryStateBackend()));

    tEnv.createTemporaryFunction("Obfuscate", Obfuscate.class);
  }

  private void createSource(String type, Row... inputData) {
    final String createSource =
        String.format(
            "CREATE TABLE input ( \n"
                + "  `field` "
                + type
                + "\n"
                + ") WITH (\n"
                + "  'connector' = 'values',\n"
                + "  'data-id' = '%s'\n"
                + ")",
            TestValuesTableFactory.registerData(Arrays.asList(inputData)));
    tEnv.executeSql(createSource);
  }

  private List<Row> executeSql() throws Exception {
    TableResult resultTable = tEnv.executeSql("SELECT Obfuscate(field) FROM input");
    return getRowsFromTable(resultTable);
  }

  @Test
  public void testNameString1() throws Exception {
    createSource("STRING", Row.of("john"), Row.of("alice"), Row.of("bob"));

    List<Row> rawResult = executeSql();

    assertThat(
        rawResult,
        containsInAnyOrder(
            Row.ofKind(RowKind.INSERT, "527bd5"),
            Row.ofKind(RowKind.INSERT, "6384e2"),
            Row.ofKind(RowKind.INSERT, "9f9d51")));
  }

  @Test
  public void testNameArray1() throws Exception {
    createSource(
        "ARRAY<STRING>",
        Row.of((Object) new String[] {"john"}),
        Row.of((Object) new String[] {"alice", "alice"}),
        Row.of((Object) new String[] {"bob"}));

    List<Row> rawResult = executeSql();

    assertThat(
        rawResult,
        containsInAnyOrder(
            Row.ofKind(RowKind.INSERT, (Object) new String[] {"527bd5"}),
            Row.ofKind(RowKind.INSERT, (Object) new String[] {"6384e2", "6384e2"}),
            Row.ofKind(RowKind.INSERT, (Object) new String[] {"9f9d51"})));
  }

  @Test
  public void testEmailString1() throws Exception {
    createSource(
        "STRING", Row.of("john@test.com"), Row.of("alice@test.com"), Row.of("bob@test.com"));

    List<Row> rawResult = executeSql();

    assertThat(
        rawResult,
        containsInAnyOrder(
            Row.ofKind(RowKind.INSERT, "527bd5@test.com"),
            Row.ofKind(RowKind.INSERT, "6384e2@test.com"),
            Row.ofKind(RowKind.INSERT, "9f9d51@test.com")));
  }

  @Test
  public void testEmailArray1() throws Exception {
    createSource(
        "ARRAY<STRING>",
        Row.of((Object) new String[] {"john@test.com"}),
        Row.of((Object) new String[] {"alice@test.com", "alice@apache.org"}),
        Row.of((Object) new String[] {"bob@test2.com"}));

    List<Row> rawResult = executeSql();

    assertThat(
        rawResult,
        containsInAnyOrder(
            Row.ofKind(RowKind.INSERT, (Object) new String[] {"527bd5@test.com"}),
            Row.ofKind(
                RowKind.INSERT, (Object) new String[] {"6384e2@test.com", "6384e2@apache.org"}),
            Row.ofKind(RowKind.INSERT, (Object) new String[] {"9f9d51@test2.com"})));
  }
}
