<!-- markdownlint-disable MD033 MD041 -->

<p align="center">
  <img src="https://raw.githubusercontent.com/JasirVoriya/images-bed/master/image/KE.png" width="200" height="200" alt="kafka_exporter">
</p>


<div align="center">

# Kafka Exporter

_✨ For Prometheus ✨_

</div>

<p align="center">
  <a href="https://www.apache.org/licenses/LICENSE-2.0.html">
    <img src="https://img.shields.io/badge/license-Apache%202-4EB1BA.svg" alt="license">
  </a>
  <img src="https://img.shields.io/badge/JDK-21+-blue" alt="Java">
  <br />
  <img src="https://img.shields.io/badge/Kafka Exporter-unstable-black?style=social&logo=data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAEAAAABABAMAAABYR2ztAAAAIVBMVEUAAAAAAAADAwMHBwceHh4UFBQNDQ0ZGRkoKCgvLy8iIiLWSdWYAAAAAXRSTlMAQObYZgAAAQVJREFUSMftlM0RgjAQhV+0ATYK6i1Xb+iMd0qgBEqgBEuwBOxU2QDKsjvojQPvkJ/ZL5sXkgWrFirK4MibYUdE3OR2nEpuKz1/q8CdNxNQgthZCXYVLjyoDQftaKuniHHWRnPh2GCUetR2/9HsMAXyUT4/3UHwtQT2AggSCGKeSAsFnxBIOuAggdh3AKTL7pDuCyABcMb0aQP7aM4AnAbc/wHwA5D2wDHTTe56gIIOUA/4YYV2e1sg713PXdZJAuncdZMAGkAukU9OAn40O849+0ornPwT93rphWF0mgAbauUrEOthlX8Zu7P5A6kZyKCJy75hhw1Mgr9RAUvX7A3csGqZegEdniCx30c3agAAAABJRU5ErkJggg==" alt="kafka_exporter">
  <a href="https://docs.github.com/en/developers/apps">
    <img src="https://img.shields.io/badge/GitHub-Kafka Expporter-181717?style=social&logo=github" alt="github"/>
  </a>
  <br />
</p>

<p align="center">
Based on the kafka-0.11.0.3 version of the monitoring index exporter, support prometheus monitoring.
</p>

## Get started

### Building

`mvn clean package` to build.

Then you will get `kafka_exporter-*.jar` and `conf.yaml` in the `target` directory:

```shell
├──   target
│   ├──  kafka_exporter-*.jar
└── └──  conf.yaml
```

### Running

```shell
java -jar  ./target/kafka_exporter-*.jar
```

The above command will load the `conf.yaml` file in the current directory and start the exporter, and you will see the following log:

```shell
[main][INFO] [2024-05-28 15:15:01] cn.voriya.kafka.metrics.ExporterApplication.main(16) | cluster: ConfigCluster(name=test1, brokers=[127.0.0.1:9092, 127.0.0.2:9092, 127.0.0.3:9092, 127.0.0.4:9092])
[main][INFO] [2024-05-28 15:15:01] cn.voriya.kafka.metrics.ExporterApplication.main(16) | cluster: ConfigCluster(name=test2, brokers=[127.0.0.5:9092, 127.0.0.6:9092, 127.0.0.7:9092, 127.0.0.8:9092])
[main][INFO] [2024-05-28 15:26:23] cn.voriya.kafka.metrics.collectors.KafkaCollector.collect(35) | Start to collect kafka metrics, cluster: [test1]
[main][INFO] [2024-05-28 15:26:26] cn.voriya.kafka.metrics.collectors.KafkaCollector.collect(38) | Finish to collect kafka metrics, cluster: [test1], time: 3069ms
[main][INFO] [2024-05-28 15:26:26] cn.voriya.kafka.metrics.collectors.KafkaCollector.collect(35) | Start to collect kafka metrics, cluster: [test2]
[main][INFO] [2024-05-28 15:26:27] cn.voriya.kafka.metrics.collectors.KafkaCollector.collect(38) | Finish to collect kafka metrics, cluster: [test2], time: 902ms
[main][INFO] [2024-05-28 15:26:27] cn.voriya.kafka.metrics.collectors.KafkaCollector.collect(50) | Finish to collect all kafka metrics, total time: 3975ms
[main][INFO] [2024-05-28 15:26:27] cn.voriya.kafka.metrics.ExporterApplication.main(20) | server started on port 1234
[main][INFO] [2024-05-28 15:26:27] cn.voriya.kafka.metrics.ExporterApplication.main(21) | Kafka Exporter started
```
If you see the above message, it means the exporter has started successfully, and you can access the metrics by visiting `http://localhost:1234/metrics`.

## Configures

The exporter uses `conf.yaml` to configure the kafka cluster information, you can modify the `conf.yaml` to add or
remove the kafka cluster information.

```yaml
cluster:
  - name: test1
    brokers: [ 127.0.0.1:9092,127.0.0.2:9092,127.0.0.3:9092,127.0.0.4:9092 ]
  - name: test2
    brokers: [ 127.0.0.5:9092,127.0.0.6:9092,127.0.0.7:9092,127.0.0.8:9092 ]
port: 1234
```

The `conf.yaml` file contains the following fields:

| Field             | Type   | Description                                                             | Default | Required |
|-------------------|--------|-------------------------------------------------------------------------|---------|----------|
| `cluster`         | Array  | The kafka cluster information, each cluster contains a name and brokers |         | Yes      |
| `cluster.name`    | String | The cluster name                                                        |         | Yes      |
| `cluster.brokers` | Array  | The kafka brokers information, each broker contains a host and port     |         | Yes      |
| `port`            | Number | The exporter listen port                                                | 1234    | No       |

You can add one or more kafka clusters to the `cluster` field, and the exporter will collect the metrics for each cluster.

## HTTP API

### Configuration

The exporter provides the following HTTP API to manage the exporter configuration:

| Path      | Method | Query         | Body                        | Description                    |
|-----------|--------|---------------|-----------------------------|--------------------------------|
| `/config` | GET    |               |                             | Get the exporter configuration |
| `/config` | POST   |               | `[cluster1, cluster2, ...]` | Add or update the cluster      |
| `/config` | DELETE | `clusterName` |                             | Remove cluster by name         |

The `cluster` field in the body is the same as the `cluster` field in the `conf.yaml` file, but it is a JSON string:

```json
[
  {
    "name": "cluster1",
    "brokers": [
      "127.0.0.1:9092",
      "127.0.0.2:9092"
    ]
  },
  {
    "name": "cluster2",
    "brokers": [
      "127.0.0.3:9092",
      "127.0.0.4:9092"
    ]
  }
]
```

The `clusterName` in the query is the name of the cluster you want to remove.

## Metrics

The exporter will expose the following metrics:

### Partition Metrics

| Name                                    | Exposed information                          |
|-----------------------------------------|----------------------------------------------|
| `kafka_topic_partition_offset`          | a topic's partition offset sum               |
| `kafka_consumer_topic_partition_offset` | a consumer's partition offset sum in a topic |
| `kafka_consumer_topic_partition_lag`    | a consumer's partition lag sum in a topic    |

### Broker Metrics


| Name                                 | Exposed information                            |
|--------------------------------------|------------------------------------------------|
| `kafka_topic_broker_offset`          | a topic's offset sum in a broker               |
| `kafka_consumer_broker_topic_offset` | a consumer's offset sum in a topic in a broker |
| `kafka_consumer_broker_topic_lag`    | a consumer's lag sum in a topic in a broker    |

### Topic Metrics

| Name                                 | Exposed information                        |
|--------------------------------------|--------------------------------------------|
| `kafka_topic_offset`                 | a topic's offset sum                       |
| `kafka_consumer_topic_offset`        | a consumer's offset sum in a topic         |
| `kafka_consumer_topic_lag`           | a consumer's lag sum in a topic            |

## Star ⭐
[![Stargazers over time](https://starchart.cc/JasirVoriya/kafka_exporter.svg?variant=adaptive)](https://starchart.cc/JasirVoriya/kafka_exporter)
