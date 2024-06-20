package cn.voriya.kafka.metrics.metrics;

import cn.voriya.kafka.metrics.entity.TopicGroupEntity;
import io.prometheus.client.Collector;

import java.util.LinkedList;
import java.util.List;

public class ExporterGroupTimeMetric extends Collector.MetricFamilySamples {
    public ExporterGroupTimeMetric() {
        super(
                "kafka_exporter_group_time",
                Collector.Type.GAUGE,
                "help",
                new LinkedList<>()
        );
    }

    public void add(TopicGroupEntity topicGroup) {
        this.samples.add(new Sample(
                this.name,
                List.of("cluster", "group"),
                List.of(topicGroup.getCluster(), topicGroup.getGroup()),
                topicGroup.getTime()
        ));
    }
}
