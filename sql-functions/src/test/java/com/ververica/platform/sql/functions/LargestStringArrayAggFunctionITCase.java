package com.ververica.platform.sql.functions;

import static org.apache.flink.table.planner.factories.TestValuesTableFactory.changelogRow;
import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.types.Row;
import org.junit.Before;
import org.junit.Test;

/** Integration test for {@link LargestStringArrayAggFunction}. */
public class LargestStringArrayAggFunctionITCase {

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

  private List<String> executeSql() throws InterruptedException, ExecutionException {
    // create sink
    tEnv.executeSql(
        "CREATE TABLE sink (\n"
            + "    name STRING,\n"
            + "    aliases ARRAY<String>\n"
            + ") WITH (\n"
            + "  'connector' = 'values',\n"
            + "  'sink-insert-only' = 'false',\n"
            + "  'changelog-mode' = 'I,UA,UB,D'\n"
            + ")");

    // run test
    tEnv.executeSql(
            "INSERT INTO sink\n"
                + "SELECT\n"
                + "  name,\n"
                + "  LargestStringArrayAggFunction(aliases)\n"
                + "FROM input\n"
                + "GROUP BY name")
        .await();

    return TestValuesTableFactory.getRawResults("sink");
  }

  @Test
  public void aggregation1() throws ExecutionException, InterruptedException {
    createSource(
        changelogRow("+I", "john", new String[] {"john@test.com"}),
        changelogRow("+I", "john", new String[] {"john@test.com", "john@apache.org"}),
        changelogRow("+I", "john", new String[] {"john@test2.com"}),
        changelogRow(
            "+I", "john", new String[] {"john@test.com", "john@apache.org", "john@mail.ru"}));

    List<String> rawResult = executeSql();

    List<String> expected =
        Arrays.asList(
            "+I(john,[john@test.com])",
            "-U(john,[john@test.com])",
            "+U(john,[john@test.com, john@apache.org])",
            "-U(john,[john@test.com, john@apache.org])",
            "+U(john,[john@test.com, john@apache.org, john@mail.ru])");
    assertEquals(expected, rawResult);
  }

  @Test
  public void aggregation2() throws ExecutionException, InterruptedException {
    createSource(
        changelogRow("+I", "john", new String[] {"john@test.com"}),
        changelogRow("-U", "john", new String[] {"john@test.com"}),
        changelogRow("+U", "john", new String[] {"john@test.com", "john@apache.org"}),
        changelogRow("-U", "john", new String[] {"john@test.com", "john@apache.org"}),
        changelogRow("+U", "john", new String[] {"john@test2.com"}),
        changelogRow("-U", "john", new String[] {"john@test2.com"}),
        changelogRow(
            "+U", "john", new String[] {"john@test.com", "john@apache.org", "john@mail.ru"}));

    List<String> rawResult = executeSql();

    List<String> expected =
        Arrays.asList(
            "+I(john,[john@test.com])",
            "-D(john,[john@test.com])",
            "+I(john,[john@test.com, john@apache.org])",
            "-D(john,[john@test.com, john@apache.org])",
            "+I(john,[john@test2.com])",
            "-D(john,[john@test2.com])",
            "+I(john,[john@test.com, john@apache.org, john@mail.ru])");
    assertEquals(expected, rawResult);
  }
}
