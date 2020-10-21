package com.ververica.platform;

import static java.time.format.DateTimeFormatter.ISO_LOCAL_TIME;
import static java.time.temporal.ChronoField.DAY_OF_MONTH;
import static java.time.temporal.ChronoField.MONTH_OF_YEAR;
import static java.time.temporal.ChronoField.YEAR;

import com.ververica.platform.io.source.ApacheMboxSource;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.SignStyle;
import java.time.temporal.ChronoField;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class FlinkMailingListToKafka {

  public static final DateTimeFormatter DATE_OR_DATETIME_FORMATTER =
      new DateTimeFormatterBuilder()
          .parseCaseInsensitive()
          .appendValue(YEAR, 4, 10, SignStyle.EXCEEDS_PAD)
          .appendLiteral('-')
          .appendValue(MONTH_OF_YEAR, 2)
          .optionalStart()
          .appendLiteral('-')
          .appendValue(DAY_OF_MONTH, 2)
          .optionalStart()
          .appendLiteral('T')
          .append(ISO_LOCAL_TIME)
          .appendLiteral('Z')
          .optionalEnd()
          .optionalEnd()
          .parseDefaulting(ChronoField.DAY_OF_MONTH, 1)
          .parseDefaulting(ChronoField.HOUR_OF_DAY, 0)
          .toFormatter();

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
            + "`subject` STRING\n"
            + ") WITH (\n"
            + "'connector' = 'kafka',\n"
            + "'topic' = '"
            + kafkaTopic
            + "-dev',\n"
            + "'properties.bootstrap.servers' = '"
            + kafkaServer
            + "',\n"
            + "'format' = 'json'\n"
            + ")");

    tableEnv.executeSql(
        "CREATE TABLE `mail_flink_user` (\n"
            + "`date` TIMESTAMP(3),\n"
            + "`fromEmail` STRING,\n"
            + "`fromRaw` STRING,\n"
            + "`subject` STRING\n"
            + ") WITH (\n"
            + "'connector' = 'kafka',\n"
            + "'topic' = '"
            + kafkaTopic
            + "-user',\n"
            + "'properties.bootstrap.servers' = '"
            + kafkaServer
            + "',\n"
            + "'format' = 'json'\n"
            + ")");

    tableEnv.executeSql(
        "CREATE TABLE `mail_flink_user_zh` (\n"
            + "`date` TIMESTAMP(3),\n"
            + "`fromEmail` STRING,\n"
            + "`fromRaw` STRING,\n"
            + "`subject` STRING\n"
            + ") WITH (\n"
            + "'connector' = 'kafka',\n"
            + "'topic' = '"
            + kafkaTopic
            + "-user-zh',\n"
            + "'properties.bootstrap.servers' = '"
            + kafkaServer
            + "',\n"
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
    final ApacheMboxSource apacheMailinglistSource;
    if (startDateString.isEmpty()) {
      apacheMailinglistSource =
          new ApacheMboxSource(listName, LocalDateTime.now(), delayBetweenQueries);
    } else {
      LocalDateTime startDate = LocalDateTime.parse(startDateString, DATE_OR_DATETIME_FORMATTER);
      apacheMailinglistSource = new ApacheMboxSource(listName, startDate, delayBetweenQueries);
    }
    return apacheMailinglistSource;
  }
}
