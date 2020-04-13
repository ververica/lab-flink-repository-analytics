![](https://github.com/ververica/lab-flink-repository-analytics/workflows/build/badge.svg)

# Apache Flink Repository Analytics with Apache Flink

This Apache Flink application analyzes the commit history of the Apache Flink Open Source project. 
It pulls data form the Github API via a custom Flink Source and writes it to ElasticSearch.

It is the running example fo a blog post series on Ververica Platform Community Edition.  

The entrypoint class is `com.ververica.platform.FlinkCommitProgram`.

## Configuration

The applications accepts the following command line arguments of the form ``--values key``.

Parameter | Description | Default
----------| ------------|--------
es-host | Elastic Search Host | elasticsearch-master-headless.vvp.svc
es-port | Elastic Search Port | 9200
start-date | The application will process the commit history of Apache Flink starting from this date. Example: "2019-01-01T00:00:00.00Z" | now 
poll-interval | Minimum pause between polling commits via  Github API in milliseconds| 1000


 