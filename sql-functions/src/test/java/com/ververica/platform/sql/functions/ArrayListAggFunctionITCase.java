package com.ververica.platform.sql.functions;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.types.Row;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/** Integration test for {@link ArrayListAggFunction} and {@link ArrayListAggFunction2}. */
@RunWith(Parameterized.class)
public class ArrayListAggFunctionITCase {

  protected StreamExecutionEnvironment env;
  protected StreamTableEnvironment tEnv;

  @Parameterized.Parameter public Class<UserDefinedFunction> implementation;

  @Parameterized.Parameters(name = "implementation = {0}")
  public static Iterable<Class<? extends UserDefinedFunction>> parameters() {
    return Arrays.asList(ArrayListAggFunction.class, ArrayListAggFunction2.class);
  }

  @Before
  public void setUp() {
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

  private List<String> executeSql() throws InterruptedException, ExecutionException {
    // create sink
    tEnv.executeSql(
        "CREATE TABLE sink (\n"
            + "    age INT,\n"
            + "    names ARRAY<String>\n"
            + ") WITH (\n"
            + "  'connector' = 'values',\n"
            + "  'sink-insert-only' = 'false',\n"
            + "  'changelog-mode' = 'I,UA,UB,D'\n"
            + ")");

    // run test
    tEnv.executeSql(
            "INSERT INTO sink\n"
                + "SELECT\n"
                + "  age,\n"
                + "  ArrayListAggFunction(DISTINCT name)\n"
                + "FROM input\n"
                + "GROUP BY age")
        .await();

    return TestValuesTableFactory.getRawResults("sink");
  }

  @Test
  public void aggregation1() throws ExecutionException, InterruptedException {
    createSource(Row.of("john", 35), Row.of("alice", 32), Row.of("bob", 35), Row.of("sarah", 32));

    List<String> rawResult = executeSql();

    String[] expected =
        new String[] {
          "+I(35,[john])",
          "-U(35,[john])",
          "+U(35,[john, bob])",
          "+I(32,[alice])",
          "-U(32,[alice])",
          "+U(32,[alice, sarah])"
        };

    assertThat(rawResult, containsInAnyOrder(expected));
  }
}
