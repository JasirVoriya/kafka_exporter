package cn.voriya.kafka.metrics.collectors;

import cn.voriya.kafka.metrics.Config;
import cn.voriya.kafka.metrics.entity.ConsumerTopicPartitionOffsetMetric;
import cn.voriya.kafka.metrics.entity.TopicPartitionOffsetMetric;
import cn.voriya.kafka.metrics.job.ConsumerTopicPartitionOffset;
import cn.voriya.kafka.metrics.job.TopicPartitionOffset;
import io.prometheus.client.Collector;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class KafkaCollector extends Collector {
    public List<MetricFamilySamples> collect() {
        List<MetricFamilySamples> kafkaMetricFamilySamples = new ArrayList<>();

        ArrayList<ConsumerTopicPartitionOffsetMetric> consumerTopicPartitionOffsetMetrics = ConsumerTopicPartitionOffset.get(Config.BROKER_LIST);
        // Your code to get metrics
        ArrayList<MetricFamilySamples.Sample> consumerSamples = new ArrayList<>();
        for (ConsumerTopicPartitionOffsetMetric metric : consumerTopicPartitionOffsetMetrics) {
            consumerSamples.add(new MetricFamilySamples.Sample(
                    ConsumerTopicPartitionOffsetMetric.METRIC_NAME,
                    List.of(ConsumerTopicPartitionOffsetMetric.HEADERS),
                    Arrays.asList(metric.toArray()),
                    1
            ));
        }
        kafkaMetricFamilySamples.add(new MetricFamilySamples(
                ConsumerTopicPartitionOffsetMetric.METRIC_NAME,
                Type.GAUGE,
                "help",
                consumerSamples));
        ArrayList<TopicPartitionOffsetMetric> topicPartitionOffsetMetrics = TopicPartitionOffset.get(Config.BROKER_LIST);
        ArrayList<MetricFamilySamples.Sample> topicSamples = new ArrayList<>();
        for (TopicPartitionOffsetMetric metric : topicPartitionOffsetMetrics) {
            topicSamples.add(new MetricFamilySamples.Sample(
                    TopicPartitionOffsetMetric.METRIC_NAME,
                    List.of(TopicPartitionOffsetMetric.HEADERS),
                    Arrays.asList(metric.toArray()),
                    1
            ));
        }
        kafkaMetricFamilySamples.add(new MetricFamilySamples(
                TopicPartitionOffsetMetric.METRIC_NAME,
                Type.GAUGE,
                "help",
                topicSamples));
        return kafkaMetricFamilySamples;
    }
}