package cn.voriya.kafka.metrics.collectors;

import cn.voriya.kafka.metrics.Config;
import cn.voriya.kafka.metrics.entity.ConsumerTopicPartitionOffsetMetric;
import cn.voriya.kafka.metrics.entity.TopicPartitionOffsetMetric;
import cn.voriya.kafka.metrics.job.ConsumerTopicPartitionOffset;
import cn.voriya.kafka.metrics.job.TopicPartitionOffset;
import io.prometheus.client.Collector;

import java.util.*;

public class KafkaCollector extends Collector {
    public List<MetricFamilySamples> collect() {
        List<MetricFamilySamples> kafkaMetricFamilySamples = new ArrayList<>();
        //查询所有topic的offset
        ArrayList<TopicPartitionOffsetMetric> topicPartitionOffsetMetrics = TopicPartitionOffset.get(Config.BROKER_LIST);
        //查询所有消费者组的offset和lag
        ArrayList<ConsumerTopicPartitionOffsetMetric> consumerTopicPartitionOffsetMetrics = ConsumerTopicPartitionOffset.get(Config.BROKER_LIST);
        //三个metric
        ArrayList<MetricFamilySamples.Sample> offsetSamples = new ArrayList<>();
        ArrayList<MetricFamilySamples.Sample> consumerOffsetSamples = new ArrayList<>();
        ArrayList<MetricFamilySamples.Sample> consumerLagSamples = new ArrayList<>();
        //根据topic和partition所拼接的字符串，查找对应的TopicPartitionOffsetMetric
        Map<String, TopicPartitionOffsetMetric> topicPartitionOffsetMetricMap = new HashMap<>();
        topicPartitionOffsetMetrics.forEach(metric -> topicPartitionOffsetMetricMap.put(metric.getTopic() + "-" + metric.getPartition(), metric));
        //根据topic和partition所拼接的字符串，查找对应的ConsumerTopicPartitionOffsetMetric
        Map<String, ConsumerTopicPartitionOffsetMetric> consumerTopicPartitionOffsetMetricMap = new HashMap<>();
        consumerTopicPartitionOffsetMetrics.forEach(metric -> consumerTopicPartitionOffsetMetricMap.put(metric.getTopic() + "-" + metric.getPartition(), metric));
        //开始生成metric
        for (TopicPartitionOffsetMetric metric : topicPartitionOffsetMetrics) {
            //根据topic和partition所拼接的字符串，查找对应的ConsumerTopicPartitionOffsetMetric
            ConsumerTopicPartitionOffsetMetric consumerTopicPartitionOffsetMetric = consumerTopicPartitionOffsetMetricMap.get(metric.getTopic() + "-" + metric.getPartition());
            //如果找到了，将endOffset赋值给metric
            if (consumerTopicPartitionOffsetMetric != null) {
                metric.setOffset(consumerTopicPartitionOffsetMetric.getLogEndOffset());
            }
            offsetSamples.add(new MetricFamilySamples.Sample(
                    TopicPartitionOffsetMetric.METRIC_NAME,
                    List.of(TopicPartitionOffsetMetric.HEADERS),
                    Arrays.asList(metric.toArray()),
                    metric.getOffset()
            ));
        }
        for (ConsumerTopicPartitionOffsetMetric metric : consumerTopicPartitionOffsetMetrics) {
            //根据topic和partition所拼接的字符串，查找对应的TopicPartitionOffsetMetric
            TopicPartitionOffsetMetric topicPartitionOffsetMetric = topicPartitionOffsetMetricMap.get(metric.getTopic() + "-" + metric.getPartition());
            //如果找到了，将leader赋值给metric
            if (topicPartitionOffsetMetric != null) {
                metric.setLeader(topicPartitionOffsetMetric.getLeader());
            }
            consumerOffsetSamples.add(new MetricFamilySamples.Sample(
                    ConsumerTopicPartitionOffsetMetric.METRIC_NAME_OFFSET,
                    List.of(ConsumerTopicPartitionOffsetMetric.HEADERS),
                    Arrays.asList(metric.toArray()),
                    metric.getOffset()
            ));
            consumerLagSamples.add(new MetricFamilySamples.Sample(
                    ConsumerTopicPartitionOffsetMetric.METRIC_NAME_LAG,
                    List.of(ConsumerTopicPartitionOffsetMetric.HEADERS),
                    Arrays.asList(metric.toArray()),
                    metric.getLag()
            ));
        }
        kafkaMetricFamilySamples.add(new MetricFamilySamples(
                TopicPartitionOffsetMetric.METRIC_NAME,
                Type.GAUGE,
                "help",
                offsetSamples));
        kafkaMetricFamilySamples.add(new MetricFamilySamples(
                ConsumerTopicPartitionOffsetMetric.METRIC_NAME_OFFSET,
                Type.GAUGE,
                "help",
                consumerOffsetSamples));
        kafkaMetricFamilySamples.add(new MetricFamilySamples(
                ConsumerTopicPartitionOffsetMetric.METRIC_NAME_LAG,
                Type.GAUGE,
                "help",
                consumerLagSamples));
        return kafkaMetricFamilySamples;
    }
}