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

/** Integration test for {@link ExpandJiraTicketComponents}. */
public class ExpandJiraTicketComponents2ITCase extends AbstractTableTestBase {

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

    tEnv.createTemporaryFunction("ExpandJiraTicketComponents", ExpandJiraTicketComponents2.class);
  }

  private void createSource(Row... inputData) {
    final String createSource =
        String.format(
            "CREATE TABLE input ( \n"
                + "  `textBody` STRING\n"
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
            "SELECT component, componentCount FROM input LEFT JOIN LATERAL TABLE(ExpandJiraTicketComponents(textBody)) ON TRUE");
    return getRowsFromTable(resultTable);
  }

  @Test
  public void testWithString1() throws Exception {
    createSource(Row.of(""));

    List<Row> rawResult = executeSql();

    assertThat(rawResult, containsInAnyOrder(Row.ofKind(RowKind.INSERT, null, null)));
  }

  @Test
  public void testWithString2() throws Exception {
    createSource(Row.of("          Components: Tests"));

    List<Row> rawResult = executeSql();

    assertThat(rawResult, containsInAnyOrder(Row.ofKind(RowKind.INSERT, "Tests", 1)));
  }

  @Test
  public void testWithString3() throws Exception {
    createSource(
        Row.of("          Components: Tests, Formats (JSON, Avro, Parquet, ORC, SequenceFile)"));

    List<Row> rawResult = executeSql();

    assertThat(
        rawResult,
        containsInAnyOrder(
            Row.ofKind(RowKind.INSERT, "Tests", 2),
            Row.ofKind(RowKind.INSERT, "Formats (JSON, Avro, Parquet, ORC, SequenceFile)", 2)));
  }

  @Test
  public void testWithString4() throws Exception {
    createSource(
        Row.of("          Components: Tests"),
        Row.of("          Components: Formats (JSON, Avro, Parquet, ORC, SequenceFile)"));

    List<Row> rawResult = executeSql();

    assertThat(
        rawResult,
        containsInAnyOrder(
            Row.ofKind(RowKind.INSERT, "Tests", 1),
            Row.ofKind(RowKind.INSERT, "Formats (JSON, Avro, Parquet, ORC, SequenceFile)", 1)));
  }
}
