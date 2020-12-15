![](../../workflows/Publish/badge.svg)

# Apache Flink Repository Analytics with Apache Flink

This Apache Flink application analyzes the commit history of the Apache Flink Open Source project. 
It pulls data form the Github API via a custom Flink Source and writes it to ElasticSearch.

It is the running example for a blog post series on Ververica Platform Community Edition.  

The entrypoint class is `com.ververica.platform.FlinkCommitProgram`.

## Configuration

The applications accepts the following command line arguments of the form ``--values key``.

Parameter | Description | Default
----------| ------------|--------
es-host | Elastic Search Host | elasticsearch-master-headless.vvp.svc
es-port | Elastic Search Port | 9200
start-date | The application will process the commit history of Apache Flink starting from this date. Example: "2019-01-01" | now
enable-new-feature | A feature flag for enabling a new Job feature | unset 
poll-interval-ms | Minimum pause between polling commits via  Github API in milliseconds| 1000
checkpointing-interval-ms | Apache Flink checkpointing interval | 10_000
