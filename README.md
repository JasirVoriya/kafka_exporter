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

If you see the above message, it means the exporter has started successfully, and you can access the metrics by
visiting `http://localhost:4399/metrics`.

## Configures

The exporter uses `conf.yaml` to configure the kafka cluster information, you can modify the `conf.yaml` to add or
remove the kafka cluster information.

```yaml
cluster:
  - name: test1
    brokers: [ 127.0.0.1:9092,127.0.0.2:9092,127.0.0.3:9092,127.0.0.4:9092 ]
    zookeepers: [ 127.0.0.1:2181,127.0.0.2:2181,127.0.0.3:2181,127.0.0.4:2181 ]
  - name: test2
    brokers: [ 127.0.0.5:9092,127.0.0.6:9092,127.0.0.7:9092,127.0.0.8:9092 ]
    zookeepers: [ 127.0.0.5:2181,127.0.0.6:2181,127.0.0.7:2181,127.0.0.8:2181 ]
port: 1234
```

The `conf.yaml` file contains the following fields:

| Field                | Type   | Description                                                              | Default | Required |
|----------------------|--------|--------------------------------------------------------------------------|---------|----------|
| `cluster`            | Array  | The kafka cluster information, each cluster contains a name and brokers  |         | Yes      |
| `cluster.name`       | String | The cluster name                                                         |         | Yes      |
| `cluster.brokers`    | Array  | The kafka brokers information, each broker contains a host and port      |         | Yes      |
| `cluster.zookeepers` | Array  | The kafka zookeeper information, each zookeeper contains a host and port |         | Yes      |
| `port`               | Number | The exporter listen port                                                 | 4399    | No       |
| `interval`           | Number | The interval to collect the metrics, in seconds                          | 15      | No       |

You can add one or more kafka clusters to the `cluster` field, and the exporter will collect the metrics for each cluster.

## HTTP API

### Configuration

1. Get the exporter configuration:

    * API:

   | Path      | Method | Description                    |
            |-----------|--------|--------------------------------|
   | `/config` | GET    | Get the exporter configuration |

    * Example:

    ```shell
    curl http://localhost:4399/config
    ```
2. Add or update the cluster:

    * API:

   | Path      | Method  | Description               |
            |-----------|---------|---------------------------|
   | `/config` | POST    | Add or update the cluster |

    * Body:

    ```json
   [
      {
        "name": "test1",
        "brokers": [
          "127.0.0.1:9092"
        ],
        "zookeepers": [
          "127.0.0.1:2181"
        ]
      }
   ]
    ```

    * Example:

    ```shell
    curl --location --request POST 'http://localhost:4399/config' \
    --header 'Content-Type: application/json' \
    --header 'Accept: */*' \
    --header 'Host: localhost:4399' \
    --data-raw '[
        {
            "name": "test1",
            "brokers": [
                "127.0.0.1:9092"
            ],
            "zookeepers": [
                "127.0.0.1:2181"
            ]
        }
    ]'
    ```
3. Remove the cluster:

    * API:

   | Path      | Method  | Description               |
            |-----------|---------|---------------------------|
   | `/config` | DELETE  | Remove cluster by name    |

    * Query:

   | Query         | Type   | Description      | Default | Required |
            |---------------|--------|------------------|---------|----------|
   | `clusterName` | String | The cluster name |         | Yes      |

    * Example:

    ```shell
    curl --location --request DELETE 'http://localhost:4399/config?cluster=test1'
    ```

### Consumer Group Information

1. Get the consumer group information:

    * API:

   | Path               | Method | Description                        |
            |--------------------|--------|------------------------------------|
   | `/consumer-groups` | GET    | Get the consumer group information |

    * Query:

   | Query     | Type   | Description      | Default | Required |
            |-----------|--------|------------------|---------|----------|
   | `cluster` | String | The cluster name |         | Yes      |

    * Example:

    ```shell
    curl http://localhost:4399/consumer-groups?cluster=test1
    ```

2. Get the consumer group failed count:

    * API:

   | Path                   | Method | Description                         |
            |------------------------|--------|-------------------------------------|
   | `/consumer-fail-count` | GET    | Get the consumer group failed count |

    * Query:

   | Query       | Type   | Description                                | Default        | Required |
            |-------------|--------|--------------------------------------------|----------------|----------|
   | `cluster`   | String | The cluster name                           |                | Yes      |
   | `min-count` | Number | The minimum count of failed consumer group | 0              | No       |
   | `max-count` | Number | The maximum count of failed consumer group | Long.MAX_VALUE | No       |

    * Example:

    ```shell
    curl http://localhost:4399/consumer-fail-count?cluster=test1&min-count=1&max-count=10
    ```

## Metrics

The exporter will expose the following metrics:

### Exporter Metrics

| Name                              | Exposed information                             |
|-----------------------------------|-------------------------------------------------|
| `kafka_exporter_group_time`       | The time to collect each consumer group metrics |
| `kafka_exporter_cluster_time`     | The time to collect each cluster metrics        |
| `kafka_exporter_total_time`       | The total time to collect all metrics           |
| `kafka_exporter_group_fail_count` | The failed consumer group count                 |

### Producer Metrics

| Name                          | Exposed information        |
|-------------------------------|----------------------------|
| `kafka_topic_producer_offset` | A partition produce offset |

### Consumer Metrics

| Name                          | Exposed information                  |
|-------------------------------|--------------------------------------|
| `kafka_topic_consumer_offset` | Every consumer offset in a partition |
| `kafka_topic_consumer_lag`    | Every consumer lag in a partition    |  

### Consumer Group Metrics

| Name                                | Exposed information                        |
|-------------------------------------|--------------------------------------------|
| `kafka_topic_consumer_group_offset` | Every consumer group offset in a partition |
| `kafka_topic_consumer_group_lag`    | Every consumer group lag in a partition    |

## Star ⭐
[![Stargazers over time](https://starchart.cc/JasirVoriya/kafka_exporter.svg?variant=adaptive)](https://starchart.cc/JasirVoriya/kafka_exporter)
