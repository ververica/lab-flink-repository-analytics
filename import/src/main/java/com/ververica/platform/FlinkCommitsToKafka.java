package com.ververica.platform;

import static com.ververica.platform.Utils.localDateTimeToInstant;

import com.ververica.platform.entities.Commit;
import com.ververica.platform.io.source.GithubCommitSource;
import java.time.Instant;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class FlinkCommitsToKafka {

  public static final String APACHE_FLINK_REPOSITORY = "apache/flink";

  public static void main(String[] args) {
    ParameterTool params = ParameterTool.fromArgs(args);

    // Sink
    String kafkaServer = params.get("kafka-server", "kafka.vvp.svc");
    String kafkaTopic = params.get("kafka-topic", "flink-commits");

    // Source
    long delayBetweenQueries = params.getLong("poll-interval-ms", 10_000L);
    String startDateString = params.get("start-date", "");

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    EnvironmentSettings settings =
        EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
    StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

    env.getConfig().enableObjectReuse();

    DataStream<Commit> commits =
        env.addSource(getGithubCommitSource(delayBetweenQueries, startDateString))
            .name("flink-commit-source")
            .uid("flink-commit-source");

    tableEnv.executeSql(
        "CREATE TABLE commits (\n"
            + "`author` STRING,\n"
            + "`authorDate` TIMESTAMP(3),\n"
            + "`authorEmail` STRING,\n"
            + "`commitDate` TIMESTAMP(3),\n"
            + "`committer` STRING,\n"
            + "`committerEmail` STRING,\n"
            + "`filesChanged` ARRAY<ROW<filename STRING, linesAdded INT, linesChanged INT, linesRemoved INT>>,\n"
            + "`sha1` STRING,\n"
            + "`shortInfo` STRING\n"
            + ") WITH (\n"
            + "'connector' = 'kafka',\n"
            + "'topic' = '"
            + kafkaTopic
            + "',\n"
            + "'properties.bootstrap.servers' = '"
            + kafkaServer
            + "',\n"
            + "'format' = 'json'\n"
            + ")");

    tableEnv.fromDataStream(commits).executeInsert("commits");
  }

  private static GithubCommitSource getGithubCommitSource(
      final long delayBetweenQueries, final String startDateString) {
    Instant startDate = localDateTimeToInstant(Utils.parseFlexibleDate(startDateString));
    return new GithubCommitSource(APACHE_FLINK_REPOSITORY, startDate, delayBetweenQueries);
  }
}
