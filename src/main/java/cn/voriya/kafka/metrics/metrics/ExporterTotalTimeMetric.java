package cn.voriya.kafka.metrics.metrics;

import io.prometheus.client.Collector;

import java.util.ArrayList;
import java.util.LinkedList;

public class ExporterTotalTimeMetric extends Collector.MetricFamilySamples {
    public ExporterTotalTimeMetric() {
        super(
                "kafka_exporter_total_time",
                Collector.Type.GAUGE,
                "help",
                new LinkedList<>()
        );
    }

    public void add(long time) {
        this.samples.add(new Sample(
                this.name,
                new ArrayList<>(),
                new ArrayList<>(),
                time
        ));
    }
}
