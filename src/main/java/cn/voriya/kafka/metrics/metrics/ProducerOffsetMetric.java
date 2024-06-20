package cn.voriya.kafka.metrics.metrics;

import cn.voriya.kafka.metrics.config.ConfigCluster;
import cn.voriya.kafka.metrics.entity.TopicProduceEntity;
import io.prometheus.client.Collector;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

public class ProducerOffsetMetric extends Collector.MetricFamilySamples {
    public ProducerOffsetMetric() {
        super(
                "kafka_topic_producer_offset",
                Collector.Type.GAUGE,
                "help",
                new LinkedList<>()
        );
    }

    public void add(TopicProduceEntity res, ConfigCluster configCluster) {
        this.samples.add(new Sample(
                this.name,
                List.of("cluster", "topic", "partition", "leader"),
                Arrays.asList(configCluster.getName(), res.getTopic(), String.valueOf(res.getPartition()), res.getLeader()),
                res.getOffset()
        ));
    }
}
