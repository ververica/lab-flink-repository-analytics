package com.ververica.platform.sql.functions;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

import java.time.LocalDateTime;
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

/** Integration test for {@link GetEmailAliasesAndCompany}. */
public class GetEmailAliasesAndCompanyITCase extends AbstractTableTestBase {

  protected StreamExecutionEnvironment env;
  protected StreamTableEnvironment tEnv;

  @Before
  public void setUp() {
    TestValuesTableFactory.clearAllData();
    env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(4);
    tEnv =
        StreamTableEnvironment.create(
            env, EnvironmentSettings.newInstance().inStreamingMode().build());
    env.getConfig().setRestartStrategy(RestartStrategies.noRestart());
    env.setStateBackend(new RocksDBStateBackend((StateBackend) new MemoryStateBackend()));

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

  private List<Row> executeSql() throws Exception {
    TableResult resultTable =
        tEnv.executeSql(
            "SELECT\n"
                + "  name,\n"
                + "  GetEmailAliasesAndCompany(email, rowtime).aliases,\n"
                + "  GetEmailAliasesAndCompany(email, rowtime).company,\n"
                + "  GetEmailAliasesAndCompany(email, rowtime).companySince\n"
                + "FROM input\n"
                + "GROUP BY name");
    return getRowsFromTable(resultTable);
  }

  @Test
  public void aggregation1() throws Exception {
    LocalDateTime t1 = LocalDateTime.parse("2021-04-01T00:00:01");
    LocalDateTime t2 = LocalDateTime.parse("2021-04-01T00:00:02");
    LocalDateTime t3 = LocalDateTime.parse("2021-04-01T00:00:03");
    LocalDateTime t4 = LocalDateTime.parse("2021-04-01T00:00:04");
    LocalDateTime t5 = LocalDateTime.parse("2021-04-01T00:00:05");
    createSource(
        Row.of("john", "john@test.com", t1),
        Row.of("john", "john@apache.org", t2),
        Row.of("john", "john@mail.ru", t3),
        Row.of("john", "john@test2.com", t4),
        Row.of("john", "john@test.com", t5));

    assertThat(
        executeSql(),
        containsInAnyOrder(
            Row.ofKind(RowKind.INSERT, "john", new String[] {"john@test.com"}, "test.com", t1),
            Row.ofKind(
                RowKind.UPDATE_BEFORE, "john", new String[] {"john@test.com"}, "test.com", t1),
            Row.ofKind(
                RowKind.UPDATE_AFTER,
                "john",
                new String[] {"john@test.com", "john@apache.org"},
                "test.com",
                t1),
            Row.ofKind(
                RowKind.UPDATE_BEFORE,
                "john",
                new String[] {"john@test.com", "john@apache.org"},
                "test.com",
                t1),
            Row.ofKind(
                RowKind.UPDATE_AFTER,
                "john",
                new String[] {"john@mail.ru", "john@test.com", "john@apache.org"},
                "test.com",
                t1),
            Row.ofKind(
                RowKind.UPDATE_BEFORE,
                "john",
                new String[] {"john@mail.ru", "john@test.com", "john@apache.org"},
                "test.com",
                t1),
            Row.ofKind(
                RowKind.UPDATE_AFTER,
                "john",
                new String[] {"john@mail.ru", "john@test2.com", "john@test.com", "john@apache.org"},
                "test2.com",
                t4),
            Row.ofKind(
                RowKind.UPDATE_BEFORE,
                "john",
                new String[] {"john@mail.ru", "john@test2.com", "john@test.com", "john@apache.org"},
                "test2.com",
                t4),
            Row.ofKind(
                RowKind.UPDATE_AFTER,
                "john",
                new String[] {"john@mail.ru", "john@test2.com", "john@test.com", "john@apache.org"},
                "test.com",
                t5)));
  }

  @Test
  public void aggregation2() throws Exception {
    LocalDateTime t6 = LocalDateTime.parse("2021-04-01T00:00:06");
    LocalDateTime t7 = LocalDateTime.parse("2021-04-01T00:00:07");
    createSource(
        Row.of("alice", "alice@apache.org", t6), Row.of("alice", "alice@data-artisans.com", t7));

    assertThat(
        executeSql(),
        containsInAnyOrder(
            Row.ofKind(RowKind.INSERT, "alice", new String[] {"alice@apache.org"}, null, null),
            Row.ofKind(
                RowKind.UPDATE_BEFORE, "alice", new String[] {"alice@apache.org"}, null, null),
            Row.ofKind(
                RowKind.UPDATE_AFTER,
                "alice",
                new String[] {"alice@data-artisans.com", "alice@apache.org"},
                "ververica.com",
                t7)));
  }
}
