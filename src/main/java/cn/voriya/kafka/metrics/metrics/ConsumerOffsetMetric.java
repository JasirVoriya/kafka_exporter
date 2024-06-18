package cn.voriya.kafka.metrics.metrics;

import cn.voriya.kafka.metrics.config.ConfigCluster;
import cn.voriya.kafka.metrics.entity.TopicConsumerResponse;
import io.prometheus.client.Collector;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

public class ConsumerOffsetMetric extends Collector.MetricFamilySamples {
    public ConsumerOffsetMetric() {
        super(
                "kafka_topic_consumer_offset",
                Collector.Type.GAUGE,
                "help",
                new LinkedList<>()
        );
    }

    public void add(TopicConsumerResponse res, ConfigCluster configCluster) {
        this.samples.add(new Sample(
                this.name,
                List.of("cluster", "consumer_group", "topic", "partition", "leader", "coordinator", "consumer_id", "host", "client_id"),
                Arrays.asList(configCluster.getName(), res.getConsumerGroup(), res.getTopic(), String.valueOf(res.getPartition()), res.getLeader(), res.getCoordinator(), res.getConsumerId(), res.getHost(), res.getClientId()),
                res.getOffset()
        ));
    }
}
