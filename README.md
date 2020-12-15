![](../../workflows/Publish/badge.svg)

# Apache Flink Repository Analytics with Apache Flink

This Apache Flink application analyzes the commit history of the Apache Flink Open Source project. 
It pulls data form the Github API via a custom Flink Source and writes it to ElasticSearch.

It is the running example for a blog post series on Ververica Platform Community Edition.  

A couple of jobs are implemented in this repository:

## `DataStream`-based analytics of component activity in the Flink Repository

The entrypoint class is [`com.ververica.platform.FlinkCommitProgram`](commit-analytics/src/main/java/com/ververica/platform/FlinkCommitProgram.java) in the [`commit-analytics`](commit-analytics) sub-project.

### Configuration

This application accepts the following command line arguments of the form ``--values key``:

Parameter          | Description                                   | Default
-------------------| ----------------------------------------------|--------
es-host            | Elastic Search Host                           | elasticsearch-master-headless.vvp.svc
es-port            | Elastic Search Port                           | 9200
start-date         | The application will process the commit history of Apache Flink starting from this date.<br />Examples: `2019-01`, `2019-01-01`, `2020-01-01T10:30Z`, `2020-01-01T10:30:00Z`, `2020-01-01T10:30:00.000Z` | now
enable-new-feature | A feature flag for enabling a new Job feature | unset
poll-interval-ms   | Minimum pause between polling commits via  Github API in milliseconds| 1000
checkpointing-interval-ms | Apache Flink checkpointing interval    | 10_000

## SQL-based analytics for the Flink Repository, Mailing Lists, and Pull Requests

This functionality is split into 2 projects:

- [`import`](import) to copy Flink Repository information, Mailing Lists contents, and Pull Requests information into Kafka
- [`sql-functions`](sql-functions) to offer a few helpers useful for a variety of SQL queries

### Import

The [`import`](import) sub-project contains three jobs to import data from various public sources around the
development of Apache Flink:
- [`com.ververica.platform.FlinkCommitsToKafka`](import/src/main/java/com/ververica/platform/FlinkCommitsToKafka.java)
- [`com.ververica.platform.FlinkMailingListToKafka`](import/src/main/java/com/ververica/platform/FlinkMailingListToKafka.java)
    - This will import the following [Apache Flink mailing lists](https://flink.apache.org/community.html#mailing-lists): `flink-dev`, `flink-user`, `flink-user-zh`
- [`com.ververica.platform.FlinkPullRequestsToKafka`](import/src/main/java/com/ververica/platform/FlinkPullRequestsToKafka.java)

These jobs leverage source implementations in the `DataStream` API and use the Table API to write the created
elements to Kafka. This application accepts the following command line arguments of the form ``--values key``:

Parameter                 | Description                         | Default
--------------------------| ------------------------------------|--------
kafka-server              | Kafka bootstrap server              | kafka.vvp.svc
kafka-topic               | Kafka topic to write to             | flink-commits
start-date                | The application will process the commit history of Apache Flink starting from this date.<br />Examples: `2019-01`, `2019-01-01`, `2020-01-01T10:30Z`, `2020-01-01T10:30:00Z`, `2020-01-01T10:30:00.000Z` | now
poll-interval-ms          | Minimum pause between polling commits via  Github API in milliseconds| 10000
checkpointing-interval-ms | Apache Flink checkpointing interval | 10_000

### SQL Functions

The [`sql-functions`](sql-functions) sub-project contains a few functions that
you can use to simplify your SQL queries when analyzing the repository.
You can find them in the [`com.ververica.platform.sql.functions` package](sql-functions/src/main/java/com/ververica/platform/sql/functions)
