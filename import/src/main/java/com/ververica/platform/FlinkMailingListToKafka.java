package com.ververica.platform;

import com.ververica.platform.io.source.ApacheMboxSource;
import java.time.LocalDateTime;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class FlinkMailingListToKafka {

  public static void main(String[] args) {
    ParameterTool params = ParameterTool.fromArgs(args);

    // Sink
    String kafkaServer = params.get("kafka-server", "kafka.vvp.svc");
    String kafkaTopic = params.get("kafka-topic", "flink-mail");

    // Source
    long delayBetweenQueries = params.getLong("poll-interval-ms", 10_000L);
    String startDateString = params.get("start-date", "");

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    EnvironmentSettings settings =
        EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
    StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

    env.getConfig().enableObjectReuse();

    Table emailsFlinkDev =
        tableEnv.fromDataStream(
            env.addSource(
                    getApacheMailingListSource("flink-dev", delayBetweenQueries, startDateString))
                .name("flink-dev-source")
                .uid("flink-dev-source"));

    Table emailsFlinkUser =
        tableEnv.fromDataStream(
            env.addSource(
                    getApacheMailingListSource("flink-user", delayBetweenQueries, startDateString))
                .name("flink-user-source")
                .uid("flink-user-source"));

    Table emailsFlinkUserZh =
        tableEnv.fromDataStream(
            env.addSource(
                    getApacheMailingListSource(
                        "flink-user-zh", delayBetweenQueries, startDateString))
                .name("flink-user-zh-source")
                .uid("flink-user-zh-source"));

    tableEnv.executeSql(
        "CREATE TABLE `mail_flink_dev` (\n"
            + "`date` TIMESTAMP(3),\n"
            + "`fromEmail` STRING,\n"
            + "`fromRaw` STRING,\n"
            + "`htmlBody` STRING,\n"
            + "`subject` STRING,\n"
            + "`textBody` STRING\n"
            + ") WITH (\n"
            + "'connector' = 'kafka',\n"
            + "'topic' = '"
            + kafkaTopic
            + "-dev',\n"
            + "'properties.bootstrap.servers' = '"
            + kafkaServer
            + "',\n"
            + "'properties.max.request.size' = '"
            + 20 * 1024 * 1024
            + "',"
            + "'format' = 'json'\n"
            + ")");

    tableEnv.executeSql(
        "CREATE TABLE `mail_flink_user` (\n"
            + "`date` TIMESTAMP(3),\n"
            + "`fromEmail` STRING,\n"
            + "`fromRaw` STRING,\n"
            + "`htmlBody` STRING,\n"
            + "`subject` STRING,\n"
            + "`textBody` STRING\n"
            + ") WITH (\n"
            + "'connector' = 'kafka',\n"
            + "'topic' = '"
            + kafkaTopic
            + "-user',\n"
            + "'properties.bootstrap.servers' = '"
            + kafkaServer
            + "',\n"
            + "'properties.max.request.size' = '"
            + 20 * 1024 * 1024
            + "',"
            + "'format' = 'json'\n"
            + ")");

    tableEnv.executeSql(
        "CREATE TABLE `mail_flink_user_zh` (\n"
            + "`date` TIMESTAMP(3),\n"
            + "`fromEmail` STRING,\n"
            + "`fromRaw` STRING,\n"
            + "`htmlBody` STRING,\n"
            + "`subject` STRING,\n"
            + "`textBody` STRING\n"
            + ") WITH (\n"
            + "'connector' = 'kafka',\n"
            + "'topic' = '"
            + kafkaTopic
            + "-user-zh',\n"
            + "'properties.bootstrap.servers' = '"
            + kafkaServer
            + "',\n"
            + "'properties.max.request.size' = '"
            + 20 * 1024 * 1024
            + "',"
            + "'format' = 'json'\n"
            + ")");

    tableEnv
        .createStatementSet()
        .addInsert("mail_flink_dev", emailsFlinkDev)
        .addInsert("mail_flink_user", emailsFlinkUser)
        .addInsert("mail_flink_user_zh", emailsFlinkUserZh)
        .execute();
  }

  private static ApacheMboxSource getApacheMailingListSource(
      String listName, long delayBetweenQueries, final String startDateString) {
    LocalDateTime startDate = Utils.parseFlexibleDate(startDateString);
    return new ApacheMboxSource(listName, startDate, delayBetweenQueries);
  }
}
