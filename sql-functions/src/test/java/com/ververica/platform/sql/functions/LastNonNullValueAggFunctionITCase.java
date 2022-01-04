package com.ververica.platform.sql.functions;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;

import java.util.Arrays;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.runtime.state.storage.JobManagerCheckpointStorage;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Integration test for {@link LastNonNullValueAggFunction} and {@link
 * LastNonNullValueAggFunction2}.
 */
@RunWith(Parameterized.class)
public class LastNonNullValueAggFunctionITCase extends AbstractTableTestBase {

  protected StreamExecutionEnvironment env;
  protected StreamTableEnvironment tEnv;

  @Parameterized.Parameter public Class<UserDefinedFunction> implementation;

  @Parameterized.Parameters(name = "implementation = {0}")
  public static Iterable<Class<? extends UserDefinedFunction>> parameters() {
    return Arrays.asList(LastNonNullValueAggFunction.class, LastNonNullValueAggFunction2.class);
  }

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

    tEnv.createTemporaryFunction("LastNonNullValueAggFunction", implementation);
  }

  private void createSource(
      String name,
      @Nullable TypeInformation<Row> dataStreamTypeInfo,
      Schema schema,
      Row... inputData) {
    SingleOutputStreamOperator<Row> changelogStream = env.fromElements(inputData);
    if (dataStreamTypeInfo != null) {
      changelogStream = changelogStream.returns(dataStreamTypeInfo);
    }
    tEnv.createTemporaryView(
        name, tEnv.fromChangelogStream(changelogStream, schema, ChangelogMode.insertOnly()));
  }

  private List<Row> getResult(String query) throws Exception {
    Table resultTable = tEnv.sqlQuery(query);

    DataStream<Row> resultStream = tEnv.toChangelogStream(resultTable);
    return getRowsFromDataStream(resultStream);
  }

  private void createSourceForType(
      String tableName, TypeInformation<?> aggJavaType, String aggSqlType, Row... inputData) {

    Schema schema =
        Schema.newBuilder()
            .column("f0", "STRING NOT NULL")
            .column("f1", aggSqlType)
            .primaryKey("f0")
            .build();
    createSource(tableName, Types.ROW(Types.STRING, aggJavaType), schema, inputData);
  }

  private void createSourceForType(
      TypeInformation<?> aggJavaType, String aggSqlType, Row... inputData) {
    createSourceForType("input", aggJavaType, aggSqlType, inputData);
  }

  private List<Row> getResultSimpleAgg() throws Exception {
    String query = "SELECT f0, LastNonNullValueAggFunction(f1) FROM input GROUP BY f0";
    return getResult(query);
  }

  @Test
  public void testInt() throws Exception {
    createSourceForType(
        Types.INT,
        "INT",
        Row.ofKind(RowKind.INSERT, "john", null),
        Row.ofKind(RowKind.INSERT, "john", 1),
        Row.ofKind(RowKind.INSERT, "john", 2),
        Row.ofKind(RowKind.INSERT, "john", null));

    assertThat(
        getResultSimpleAgg(),
        contains(
            Row.ofKind(RowKind.INSERT, "john", null),
            Row.ofKind(RowKind.UPDATE_BEFORE, "john", null),
            Row.ofKind(RowKind.UPDATE_AFTER, "john", 1),
            Row.ofKind(RowKind.UPDATE_BEFORE, "john", 1),
            Row.ofKind(RowKind.UPDATE_AFTER, "john", 2)));
  }

  @Test
  public void testString() throws Exception {
    createSourceForType(
        Types.STRING,
        "STRING",
        Row.ofKind(RowKind.INSERT, "john", null),
        Row.ofKind(RowKind.INSERT, "john", "1"),
        Row.ofKind(RowKind.INSERT, "john", "2"),
        Row.ofKind(RowKind.INSERT, "john", null));

    assertThat(
        getResultSimpleAgg(),
        contains(
            Row.ofKind(RowKind.INSERT, "john", null),
            Row.ofKind(RowKind.UPDATE_BEFORE, "john", null),
            Row.ofKind(RowKind.UPDATE_AFTER, "john", "1"),
            Row.ofKind(RowKind.UPDATE_BEFORE, "john", "1"),
            Row.ofKind(RowKind.UPDATE_AFTER, "john", "2")));
  }

  @Test
  public void testStringRetract() throws Exception {
    createSourceForType(
        Types.STRING,
        "STRING",
        Row.ofKind(RowKind.INSERT, "john", null),
        Row.ofKind(RowKind.INSERT, "john", "1"),
        Row.ofKind(RowKind.INSERT, "john", "2"),
        Row.ofKind(RowKind.INSERT, "john", null));

    assertThat(
        getResultSimpleAgg(),
        contains(
            Row.ofKind(RowKind.INSERT, "john", null),
            Row.ofKind(RowKind.UPDATE_BEFORE, "john", null),
            Row.ofKind(RowKind.UPDATE_AFTER, "john", "1"),
            Row.ofKind(RowKind.UPDATE_BEFORE, "john", "1"),
            Row.ofKind(RowKind.UPDATE_AFTER, "john", "2")));
  }
}
