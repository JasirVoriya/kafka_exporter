package cn.voriya.kafka.metrics.metrics;

import cn.voriya.kafka.metrics.config.ConfigCluster;
import io.prometheus.client.Collector;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class ExporterGroupFailCountMetric extends Collector.MetricFamilySamples {
    public ExporterGroupFailCountMetric() {
        super(
                "kafka_exporter_group_fail_count",
                Collector.Type.GAUGE,
                "help",
                new LinkedList<>()
        );
    }

    public void add(Map<String, Long> failCount, ConfigCluster configCluster) {
        failCount.forEach((group, count) -> this.samples.add(new Sample(
                this.name,
                List.of("cluster", "consumer_group"),
                Arrays.asList(configCluster.getName(), group),
                count
        )));
    }
}
