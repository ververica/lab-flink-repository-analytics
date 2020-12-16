package com.ververica.platform;

import static com.ververica.platform.Utils.localDateTimeToInstant;

import com.ververica.platform.entities.PullRequest;
import com.ververica.platform.io.source.GithubPullRequestSource;
import java.time.Instant;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Flink job that reads pull requests issued to the apache/flink Github repository (using the Github
 * API) and writes their metadata to Kafka.
 */
public class FlinkPullRequestsToKafka {

  public static final String APACHE_FLINK_REPOSITORY = "apache/flink";

  public static void main(String[] args) {
    ParameterTool params = ParameterTool.fromArgs(args);

    // Sink
    String kafkaServer = params.get("kafka-server", "kafka.vvp.svc");
    String kafkaTopic = params.get("kafka-topic", "flink-pulls");

    // Source
    long delayBetweenQueries = params.getLong("poll-interval-ms", 10_000L);
    String startDateString = params.get("start-date", "");

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    EnvironmentSettings settings =
        EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
    StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

    env.getConfig().enableObjectReuse();

    DataStream<PullRequest> commits =
        env.addSource(getGithubPullRequestSource(delayBetweenQueries, startDateString))
            .name("flink-pulls-source")
            .uid("flink-pulls-source");

    tableEnv.executeSql(
        "CREATE TABLE `pulls` (\n"
            + "`closedAt` TIMESTAMP(3),\n"
            + "`commentsCount` INT,\n"
            + "`commitCount` INT,\n"
            + "`createdAt` TIMESTAMP(3),\n"
            + "`creator` STRING,\n"
            + "`creatorEmail` STRING,\n"
            + "`filesChanged` ARRAY<ROW<filename STRING, linesAdded INT, linesChanged INT, linesRemoved INT>>,\n"
            + "`isMerged` BOOLEAN,\n"
            + "`linesAdded` INT,\n"
            + "`linesRemoved` INT,\n"
            + "`mergedAt` TIMESTAMP(3),\n"
            + "`mergedBy` STRING,\n"
            + "`mergedByEmail` STRING,\n"
            + "`number` INT,\n"
            + "`reviewCommentCount` INT,\n"
            + "`state` STRING,\n"
            + "`title` STRING,\n"
            + "`updatedAt` TIMESTAMP(3)\n"
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

    tableEnv.fromDataStream(commits).executeInsert("pulls");
  }

  private static GithubPullRequestSource getGithubPullRequestSource(
      final long delayBetweenQueries, final String startDateString) {
    Instant startDate = localDateTimeToInstant(Utils.parseFlexibleDate(startDateString));
    return new GithubPullRequestSource(APACHE_FLINK_REPOSITORY, startDate, delayBetweenQueries);
  }
}
