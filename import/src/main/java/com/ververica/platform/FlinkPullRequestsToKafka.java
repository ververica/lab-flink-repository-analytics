package com.ververica.platform;

import static com.ververica.platform.Utils.localDateTimeToInstant;
import static org.apache.flink.table.api.Expressions.$;

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
    String kafkaSecurityProtocol = params.get("kafka-security-protocol", null);
    String kafkaSaslMechanism = params.get("kafka-sasl-mechanism", null);
    String kafkaSaslJaasConfig = params.get("kafka-sasl-jaas-config", null);

    // Source
    long delayBetweenQueries = params.getLong("poll-interval-ms", 10_000L);
    String startDateString = params.get("start-date", "");

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
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
            + "`createdAt` TIMESTAMP(3),\n"
            + "`creator` STRING,\n"
            + "`creatorEmail` STRING,\n"
            + "`description` STRING,\n"
            + "`labels` ARRAY<STRING>,\n"
            + "`mergeCommit` STRING,\n"
            + "`mergedAt` TIMESTAMP(3),\n"
            + "`number` INT,\n"
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
            + (kafkaSecurityProtocol != null
                ? "'properties.security.protocol' = '" + kafkaSecurityProtocol + "',\n"
                : "")
            + (kafkaSaslMechanism != null
                ? "'properties.sasl.mechanism' = '" + kafkaSaslMechanism + "',\n"
                : "")
            + (kafkaSaslJaasConfig != null
                ? "'properties.sasl.jaas.config' = '" + kafkaSaslJaasConfig + "',\n"
                : "")
            + "'format' = 'json'\n"
            + ")");

    tableEnv
        .fromDataStream(
            commits,
            $("closedAt"),
            $("commentsCount"),
            $("createdAt"),
            $("creator"),
            $("creatorEmail"),
            $("description"),
            $("labels"),
            $("mergeCommit"),
            $("mergedAt"),
            $("number"),
            $("state"),
            $("title"),
            $("updatedAt"))
        .executeInsert("pulls");
  }

  private static GithubPullRequestSource getGithubPullRequestSource(
      final long delayBetweenQueries, final String startDateString) {
    Instant startDate = localDateTimeToInstant(Utils.parseFlexibleDate(startDateString));
    return new GithubPullRequestSource(APACHE_FLINK_REPOSITORY, startDate, delayBetweenQueries);
  }
}
