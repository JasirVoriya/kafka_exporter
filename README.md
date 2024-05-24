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
  <img src="https://img.shields.io/badge/Java-17+-blue" alt="Java">
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

## Building

`mvn clean package` to build.

Then you will get a `kafka_exporter-unsable.jar` in the `target` directory.

## Run

### Run Binary

```shell
java -jar  kafka_exporter-unsable.jar <host1:port1,host2:port2,host3:port3,...> <listen-port>
```

### Exsample

```shell
java -jar  kafka_exporter-unsable.jar 192.168.31.110:9092,192.168.31.111:9092,192.168.31.112:9092 1234
```

The above command will start the exporter and listen on port 1234,console will print following message:

```shell
[main][INFO] [2024-05-24 11:29:09] cn.voriya.kafka.metrics.ExporterApplication.main(19) | broker list: 192.168.31.110:9092,192.168.31.111:9092,192.168.31.112:9092
[main][INFO] [2024-05-24 11:29:09] cn.voriya.kafka.metrics.ExporterApplication.main(20) | port: 1234
[main][INFO] [2024-05-24 11:29:10] kafka.utils.Logging$class.info(70) | Verifying properties
[main][INFO] [2024-05-24 11:29:10] kafka.utils.Logging$class.info(70) | Property client.id is overridden to GetOffsetJavaAPI
[main][INFO] [2024-05-24 11:29:10] kafka.utils.Logging$class.info(70) | Property metadata.broker.list is overridden to 192.168.31.110:9092,192.168.31.111:9092,192.168.31.112:9092
[main][INFO] [2024-05-24 11:29:10] kafka.utils.Logging$class.info(70) | Property request.timeout.ms is overridden to 10000
[main][INFO] [2024-05-24 11:29:10] kafka.utils.Logging$class.info(70) | Fetching metadata from broker BrokerEndPoint(3,192.168.31.112,9092) with correlation id 100000 for 0 topic(s) ListSet()
[main][INFO] [2024-05-24 11:29:10] kafka.utils.Logging$class.info(70) | Connected to 192.168.31.112:9092 for producing
[main][INFO] [2024-05-24 11:29:10] kafka.utils.Logging$class.info(70) | Disconnecting from 192.168.31.112:9092
[main][INFO] [2024-05-24 11:29:16] cn.voriya.kafka.metrics.ExporterApplication.main(23) | server started on port 1234
[main][INFO] [2024-05-24 11:29:16] cn.voriya.kafka.metrics.ExporterApplication.main(24) | Kafka Exporter started
```
If you see the above message, it means the exporter has started successfully, and you can access the metrics by visiting `http://localhost:1234/metrics`.

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
