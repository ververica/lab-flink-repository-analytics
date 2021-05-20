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

/** Integration test for {@link ArrayRepeat}. */
public class ArrayRepeatITCase extends AbstractTableTestBase {

  protected StreamExecutionEnvironment env;
  protected StreamTableEnvironment tEnv;

  @Before
  public void setUp() {
    env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(4);
    tEnv =
        StreamTableEnvironment.create(
            env, EnvironmentSettings.newInstance().inStreamingMode().build());
    env.getConfig().setRestartStrategy(RestartStrategies.noRestart());
    env.setStateBackend(new RocksDBStateBackend((StateBackend) new MemoryStateBackend()));

    tEnv.createTemporaryFunction("ArrayRepeat", ArrayRepeat.class);
  }

  private void createSource(String type, Row... inputData) {
    final String createSource =
        String.format(
            "CREATE TABLE input ( \n"
                + "  `field` "
                + type
                + ",\n"
                + "  `repetitions` INT\n"
                + ") WITH (\n"
                + "  'connector' = 'values',\n"
                + "  'data-id' = '%s'\n"
                + ")",
            TestValuesTableFactory.registerData(Arrays.asList(inputData)));
    tEnv.executeSql(createSource);
  }

  private List<Row> executeSql() throws Exception {
    TableResult resultTable = tEnv.executeSql("SELECT ArrayRepeat(field, repetitions) FROM input");
    return getRowsFromTable(resultTable);
  }

  private void testCharType(String charType) throws Exception {
    createSource(
        charType, Row.of("john", 1), Row.of("alice", 2), Row.of("bob", null), Row.of(null, 4));

    List<Row> rawResult = executeSql();

    assertThat(
        rawResult,
        containsInAnyOrder(
            Row.ofKind(RowKind.INSERT, (Object) new String[] {"john"}),
            Row.ofKind(RowKind.INSERT, (Object) new String[] {"alice", "alice"}),
            Row.ofKind(RowKind.INSERT, (Object) null),
            Row.ofKind(RowKind.INSERT, (Object) null)));
  }

  @Test
  public void testWithString1() throws Exception {
    testCharType("STRING");
  }

  @Test
  public void testWithChar1() throws Exception {
    testCharType("CHAR(6)");
  }

  @Test
  public void testWithVarChar1() throws Exception {
    testCharType("VARCHAR(6)");
  }

  @Test
  public void testWithInt1() throws Exception {
    createSource("INT", Row.of(1, 1), Row.of(2, 2), Row.of(3, null), Row.of(null, 4));

    List<Row> rawResult = executeSql();

    assertThat(
        rawResult,
        containsInAnyOrder(
            Row.ofKind(RowKind.INSERT, (Object) new Integer[] {1}),
            Row.ofKind(RowKind.INSERT, (Object) new Integer[] {2, 2}),
            Row.ofKind(RowKind.INSERT, (Object) null),
            Row.ofKind(RowKind.INSERT, (Object) null)));
  }
}
