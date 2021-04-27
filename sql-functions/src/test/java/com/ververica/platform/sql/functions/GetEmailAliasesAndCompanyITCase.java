package com.ververica.platform.sql.functions;

import static org.junit.Assert.assertEquals;

import java.time.LocalDateTime;
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

/** Integration test for {@link GetEmailAliasesAndCompany}. */
public class GetEmailAliasesAndCompanyITCase {

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

    tEnv.createTemporaryFunction("GetEmailAliasesAndCompany", GetEmailAliasesAndCompany.class);
  }

  private void createSource(Row... inputData) {
    final String createSource =
        String.format(
            "CREATE TABLE input (\n"
                + "  `name` STRING,\n"
                + "  `email` STRING,\n"
                + "  `rowtime` TIMESTAMP\n"
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
            + "    name STRING,\n"
            + "    aliases ARRAY<String>,\n"
            + "    company STRING,\n"
            + "    companySince TIMESTAMP\n"
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
                + "  GetEmailAliasesAndCompany(email, rowtime).aliases,\n"
                + "  GetEmailAliasesAndCompany(email, rowtime).company,\n"
                + "  GetEmailAliasesAndCompany(email, rowtime).companySince\n"
                + "FROM input\n"
                + "GROUP BY name")
        .await();

    return TestValuesTableFactory.getRawResults("sink");
  }

  @Test
  public void aggregation1() throws ExecutionException, InterruptedException {
    createSource(
        Row.of("john", "john@test.com", LocalDateTime.parse("2021-04-01T00:00:01")),
        Row.of("john", "john@apache.org", LocalDateTime.parse("2021-04-01T00:00:02")),
        Row.of("john", "john@mail.ru", LocalDateTime.parse("2021-04-01T00:00:03")),
        Row.of("john", "john@test2.com", LocalDateTime.parse("2021-04-01T00:00:04")),
        Row.of("john", "john@test.com", LocalDateTime.parse("2021-04-01T00:00:05")));

    List<String> rawResult = executeSql();

    List<String> expected =
        Arrays.asList(
            "+I(john,[john@test.com],test.com,2021-04-01T00:00:01)",
            "-U(john,[john@test.com],test.com,2021-04-01T00:00:01)",
            "+U(john,[john@test.com, john@apache.org],test.com,2021-04-01T00:00:01)",
            "-U(john,[john@test.com, john@apache.org],test.com,2021-04-01T00:00:01)",
            "+U(john,[john@mail.ru, john@test.com, john@apache.org],test.com,2021-04-01T00:00:01)",
            "-U(john,[john@mail.ru, john@test.com, john@apache.org],test.com,2021-04-01T00:00:01)",
            "+U(john,[john@mail.ru, john@test2.com, john@test.com, john@apache.org],test2.com,2021-04-01T00:00:04)",
            "-U(john,[john@mail.ru, john@test2.com, john@test.com, john@apache.org],test2.com,2021-04-01T00:00:04)",
            "+U(john,[john@mail.ru, john@test2.com, john@test.com, john@apache.org],test.com,2021-04-01T00:00:05)");
    assertEquals(expected, rawResult);
  }

  @Test
  public void aggregation2() throws ExecutionException, InterruptedException {
    createSource(
        Row.of("alice", "alice@apache.org", LocalDateTime.parse("2021-04-01T00:00:06")),
        Row.of("alice", "alice@data-artisans.com", LocalDateTime.parse("2021-04-01T00:00:07")));

    List<String> rawResult = executeSql();

    List<String> expected =
        Arrays.asList(
            "+I(alice,[alice@apache.org],null,null)",
            "-U(alice,[alice@apache.org],null,null)",
            "+U(alice,[alice@data-artisans.com, alice@apache.org],ververica.com,2021-04-01T00:00:07)");
    assertEquals(expected, rawResult);
  }
}
