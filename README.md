![](../../workflows/Publish/badge.svg)

# Apache Flink Community Data Analytics with Apache Flink

This project contains a couple of tools to analyze data around the Apache Flink community, including
- the [commit history](https://github.com/apache/flink/commits/master) of the Apache Flink Open Source project,
- the [pull requests](https://github.com/apache/flink/pulls) to its repository on Github,
- and messages on the [user and developer mailing lists](https://flink.apache.org/community.html#mailing-lists)
  which also contain created Jira ticket information (on the `flink-dev` mailing list).

While there are a couple of sub-projects, the overall analytics basically splits up into two categories:
1. The original Flink Repository Analytics (via `DataStream` API), and
2. SQL analytics on any of the available data above.

## Flink Repository Analytics with the `DataStream` API

This Apache Flink application analyzes the commit history of the Apache Flink Open Source project
to determine each components' activity over time.
It pulls data live from the Github API via a custom Flink Source and writes it to ElasticSearch.

It is the running example for a [blog post series on Ververica Platform Community Edition](https://www.ververica.com/blog/analyzing-github-activity-with-ververica-platform-community-edition).

The entrypoint class is [`com.ververica.platform.FlinkCommitProgram`](commit-analytics/src/main/java/com/ververica/platform/FlinkCommitProgram.java) in the [`commit-analytics`](commit-analytics) sub-project.

### Configuration

This application accepts the following command line arguments of the form `--values key`:

Parameter          | Description                                   | Default
-------------------| ----------------------------------------------|--------
es-host            | Elastic Search Host                           | elasticsearch-master-headless.vvp.svc
es-port            | Elastic Search Port                           | 9200
start-date         | The application will process the commit history of Apache Flink starting from this date.<br />Examples: `2019-01`, `2019-01-01`, `2020-01-01T10:30Z`, `2020-01-01T10:30:00Z`, `2020-01-01T10:30:00.000Z` | now
enable-new-feature | A feature flag for enabling a new Job feature | unset
poll-interval-ms   | Minimum pause between polling commits via the Github API (in milliseconds)| 1000
checkpointing-interval-ms | Apache Flink checkpointing interval (in milliseconds) | 10000

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
elements to Kafka.

#### Configuration

Each application accepts the following command line arguments of the form `--values key`:

Parameter                 | Description                         | Default
--------------------------| ------------------------------------|--------
kafka-server              | Kafka bootstrap server              | kafka.vvp.svc
kafka-topic               | Kafka topic to write to (`FlinkMailingListToKafka` will use this as the prefix) | `flink-commits || flink-mail || flink-pulls`
start-date                | The application will process its input starting from this date.<br />Examples: `2019-01`, `2019-01-01`, `2020-01-01T10:30Z`, `2020-01-01T10:30:00Z`, `2020-01-01T10:30:00.000Z` | now
poll-interval-ms          | Minimum pause between polling input data after reaching the current date and time (in milliseconds) | 10000
checkpointing-interval-ms | Apache Flink checkpointing interval (in milliseconds) | 10000

#### Output Table Definitions

The Kafka tables that the [`import`](import) sub-project is writing to can directly be used in
SQL queries with the following table definitions that you just need to point to your Kafka server(s),
adjusting other properties as needed, e.g. Kafka topic names or watermark definitions:

##### Flink Commits

```sql
CREATE TABLE `flink_commits` (
  `author` STRING,
  `authorDate` TIMESTAMP(3),
  `authorEmail` STRING,
  `commitDate` TIMESTAMP(3),
  `committer` STRING,
  `committerEmail` STRING,
  `filesChanged` ARRAY<ROW<filename STRING, linesAdded INT, linesChanged INT, linesRemoved INT>>,
  `sha1` STRING,
  `shortInfo` STRING,
  WATERMARK FOR `commitDate` AS `commitDate` - INTERVAL '1' DAY
)
COMMENT 'Commits on the master branch of github.com/apache/flink'
WITH (
  'connector' = 'kafka',
  'topic' = 'flink-commits',
  'properties.bootstrap.servers' = '<kafka-server>',
  'properties.group.id' = 'flink-analytics',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json',
  'json.fail-on-missing-field' = 'false',
  'json.ignore-parse-errors' = 'true'
);
```

##### Flink Pull Requests

```sql
CREATE TABLE `flink_pulls` (
  `closedAt` TIMESTAMP(3),
  `commentsCount` INT,
  `commitCount` INT,
  `createdAt` TIMESTAMP(3),
  `creator` STRING,
  `creatorEmail` STRING,
  `filesChanged` ARRAY<ROW<filename STRING, linesAdded INT, linesChanged INT, linesRemoved INT>>,
  `isMerged` BOOLEAN,
  `linesAdded` INT,
  `linesRemoved` INT,
  `mergedAt` TIMESTAMP(3),
  `mergedBy` STRING,
  `mergedByEmail` STRING,
  `number` INT,
  `reviewCommentCount` INT,
  `state` STRING,
  `title` STRING,
  `updatedAt` TIMESTAMP(3),
  WATERMARK FOR `createdAt` AS `createdAt` - INTERVAL '7' DAY,
  PRIMARY KEY (`number`) NOT ENFORCED
)
COMMENT 'Pull requests opened for the master branch of github.com/apache/flink'
WITH (
  'connector' = 'kafka',
  'topic' = 'flink-pulls',
  'properties.bootstrap.servers' = '<kafka-server>',
  'properties.group.id' = 'flink-analytics',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json',
  'json.fail-on-missing-field' = 'false',
  'json.ignore-parse-errors' = 'true'
);
```

##### Flink Mailing Lists

```sql
CREATE TABLE `flink_ml_dev` (
  `date` TIMESTAMP(3),
  `fromEmail` STRING,
  `fromRaw` STRING,
  `htmlBody` STRING,
  `subject` STRING,
  `textBody` STRING,
  WATERMARK FOR `date` AS `date` - INTERVAL '1' DAY
)
COMMENT 'Email summary of all messages sent to dev@flink.apache.org>'
WITH (
  'connector' = 'kafka',
  'topic' = 'flink-mail-dev',
  'properties.bootstrap.servers' = '<kafka-server>',
  'properties.group.id' = 'flink-analytics',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json',
  'json.fail-on-missing-field' = 'false',
  'json.ignore-parse-errors' = 'true'
);

CREATE TABLE `flink_ml_user` (
  `date` TIMESTAMP(3),
  `fromEmail` STRING,
  `fromRaw` STRING,
  `htmlBody` STRING,
  `subject` STRING,
  `textBody` STRING,
  WATERMARK FOR `date` AS `date` - INTERVAL '1' DAY
)
COMMENT 'Email summary of all messages sent to user@flink.apache.org>'
WITH (
  'connector' = 'kafka',
  'topic' = 'flink-mail-user',
  'properties.bootstrap.servers' = '<kafka-server>',
  'properties.group.id' = 'flink-analytics',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json',
  'json.fail-on-missing-field' = 'false',
  'json.ignore-parse-errors' = 'true'
);

CREATE TABLE `flink_ml_user_zh` (
  `date` TIMESTAMP(3),
  `fromEmail` STRING,
  `fromRaw` STRING,
  `htmlBody` STRING,
  `subject` STRING,
  `textBody` STRING,
  WATERMARK FOR `date` AS `date` - INTERVAL '1' DAY
)
COMMENT 'Email summary of all messages sent to user-zh@flink.apache.org>'
WITH (
  'connector' = 'kafka',
  'topic' = 'flink-mail-user-zh',
  'properties.bootstrap.servers' = '<kafka-server>',
  'properties.group.id' = 'flink-analytics',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json',
  'json.fail-on-missing-field' = 'false',
  'json.ignore-parse-errors' = 'true'
);
```

### SQL Functions

The [`sql-functions`](sql-functions) sub-project contains a few user-defined functions that
you can use to simplify your SQL queries when analyzing the repository.
You can find them in the [`com.ververica.platform.sql.functions` package](sql-functions/src/main/java/com/ververica/platform/sql/functions)

### Community Data Analytics Examples

The following SQL statements are just examples of things you can look for with the data
that is available. There is much more you can find out. A few more examples were presented
in a [Flink Forward Global 2020](https://www.flink-forward.org/) talk on
[A Year in Flink - Flink SQL Live Coding](https://youtu.be/aDFv_-VHP8s).

#### Number of Distinct Users per Year on the Mailing List

```sql
SELECT
  TUMBLE_END(`date`, INTERVAL '365' DAY(3)) as windowEnd,
  COUNT(DISTINCT fromEmail) AS numUsers
FROM flink_ml_user
GROUP BY TUMBLE(`date`, INTERVAL '365' DAY(3));
```

#### Emails on the User Mailing List with no Reply within 30 Days

```sql
SELECT
  SESSION_END(`date`, INTERVAL '30' DAY) AS windowEnd,
  NormalizeEmailThread(subject) AS thread,
  COUNT(*) as numMessagesInThread
FROM flink_ml_user
WHERE `date` > (CURRENT_TIMESTAMP - INTERVAL '1' YEAR)
GROUP BY SESSION(`date`, INTERVAL '30' DAY), NormalizeEmailThread(subject)
HAVING COUNT(*) < 2;
```

#### Commit Activity per Month and Flink Component

This is basically the SQL version of the [Flink Repository Analytics with the `DataStream` API](#flink-repository-analytics-with-the-datastream-api) introduced above.

```sql
SELECT
  TUMBLE_END(commitDate, INTERVAL '30' DAY) AS windowEnd,
  GetSourceComponent(filename),
  SUM(linesChanged) AS linesChanged
FROM flink_commits CROSS JOIN UNNEST(filesChanged) AS t
WHERE commitDate > (CURRENT_TIMESTAMP - INTERVAL '1' YEAR)
GROUP BY TUMBLE(commitDate, INTERVAL '30' DAY), GetSourceComponent(filename)
HAVING SUM(linesChanged) > 1000;
```

#### Jira Created Tickets per Month and Jira Component

```sql
SELECT
  TUMBLE_END(`date`, INTERVAL '30' DAY) as windowEnd,
  component,
  COUNT(*) as createdTickets
FROM flink_ml_dev
  CROSS JOIN UNNEST(GetJiraTicketComponents(textBody)) AS c (component)
WHERE `date` > (CURRENT_TIMESTAMP - INTERVAL '1' YEAR)
  AND IsJiraTicket(fromRaw)
  AND GetJiraTicketComponents(textBody) IS NOT NULL
GROUP BY TUMBLE(`date`, INTERVAL '30' DAY), component
HAVING COUNT(*) > 10;
```

## Helper Utilities

A couple of other sub-projects mainly serve as helper utilities providing common functionality:

- [`common`](common) offers generic helper classes and all entities used throughout the whole project.
- [`source-github`](source-github) contains implementations of sources interacting with the Github API.
- [`source-mbox`](source-mbox) contains a source implementation that uses [Apache James Mime4J](https://james.apache.org/mime4j/) to parse mbox archives.
