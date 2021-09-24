package com.ververica.platform;

import static org.apache.flink.table.api.Expressions.$;

import com.ververica.platform.entities.Commit;
import com.ververica.platform.io.source.JGitCommitSource;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Flink job that reads commits in the apache/flink Github repository (using the Github API) and
 * writes commit metadata to Kafka.
 */
public class FlinkCommitsToKafka {

  public static final String APACHE_FLINK_REPOSITORY = "https://github.com/apache/flink.git";
  public static final String APACHE_FLINK_BRANCH = "refs/heads/master";

  public static void main(String[] args) {
    ParameterTool params = ParameterTool.fromArgs(args);

    // Sink
    String kafkaServer = params.get("kafka-server", "kafka.vvp.svc");
    String kafkaTopic = params.get("kafka-topic", "flink-commits");
    String kafkaSecurityProtocol = params.get("kafka-security-protocol", null);
    String kafkaSaslMechanism = params.get("kafka-sasl-mechanism", null);
    String kafkaSaslJaasConfig = params.get("kafka-sasl-jaas-config", null);

    // Source
    long delayBetweenQueries = params.getLong("poll-interval-ms", 10_000L);
    String ignoreCommitsBefore = params.get("ignore-commits-before", null);

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
    StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

    env.getConfig().enableObjectReuse();

    DataStream<Commit> commits =
        env.addSource(getGitCommitSource(delayBetweenQueries, ignoreCommitsBefore))
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
            + (kafkaSecurityProtocol != null
                ? "'properties.security.protocol' = '" + kafkaSecurityProtocol + "',\n"
                : "")
            + (kafkaSaslMechanism != null
                ? "'properties.sasl.mechanism' = '" + kafkaSaslMechanism + "',\n"
                : "")
            + (kafkaSaslJaasConfig != null
                ? "'properties.sasl.jaas.config' = '" + kafkaSaslJaasConfig + "',\n"
                : "")
            + "'properties.max.request.size' = '"
            + 5 * 1024 * 1024
            + "',"
            + "'format' = 'json'\n"
            + ")");

    tableEnv
        .fromDataStream(
            commits,
            $("author"),
            $("authorDate"),
            $("authorEmail"),
            $("commitDate"),
            $("committer"),
            $("committerEmail"),
            $("filesChanged"),
            $("sha1"),
            $("shortInfo"))
        .executeInsert("commits");
  }

  private static JGitCommitSource getGitCommitSource(
      final long delayBetweenQueries, final String ignoreCommitsBefore) {
    return new JGitCommitSource(
        APACHE_FLINK_REPOSITORY, APACHE_FLINK_BRANCH, ignoreCommitsBefore, delayBetweenQueries);
  }
}
