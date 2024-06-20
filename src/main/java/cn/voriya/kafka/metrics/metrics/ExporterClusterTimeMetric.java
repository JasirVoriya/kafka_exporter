package cn.voriya.kafka.metrics.metrics;

import io.prometheus.client.Collector;

import java.util.LinkedList;
import java.util.List;

public class ExporterClusterTimeMetric extends Collector.MetricFamilySamples {
    public ExporterClusterTimeMetric() {
        super(
                "kafka_exporter_cluster_time",
                Collector.Type.GAUGE,
                "help",
                new LinkedList<>()
        );
    }

    public void add(String cluster, long time) {
        this.samples.add(new Sample(
                this.name,
                List.of("cluster"),
                List.of(cluster),
                time
        ));
    }
}
