package cn.voriya.kafka.metrics.metrics;

import cn.voriya.kafka.metrics.config.ConfigCluster;
import cn.voriya.kafka.metrics.entity.TopicConsumerEntity;
import io.prometheus.client.Collector;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

public class ConsumerGroupLagMetric extends Collector.MetricFamilySamples {
    public ConsumerGroupLagMetric() {
        super(
                "kafka_topic_consumer_group_lag",
                Collector.Type.GAUGE,
                "help",
                new LinkedList<>()
        );
    }

    public void add(TopicConsumerEntity res, ConfigCluster configCluster) {
        this.samples.add(new Sample(
                this.name,
                List.of("cluster", "consumer_group", "topic", "partition", "leader", "coordinator"),
                Arrays.asList(configCluster.getName(), res.getConsumerGroup(), res.getTopic(), String.valueOf(res.getPartition()), res.getLeader(), res.getCoordinator()),
                res.getLag()
        ));
    }
}
