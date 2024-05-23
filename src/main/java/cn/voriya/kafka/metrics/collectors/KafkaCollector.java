package cn.voriya.kafka.metrics.collectors;

import cn.voriya.kafka.metrics.Config;
import cn.voriya.kafka.metrics.entity.ConsumerTopicPartitionOffsetMetric;
import cn.voriya.kafka.metrics.entity.TopicPartitionOffsetMetric;
import cn.voriya.kafka.metrics.job.ConsumerTopicPartitionOffset;
import cn.voriya.kafka.metrics.job.TopicPartitionOffset;
import io.prometheus.client.Collector;
import lombok.extern.slf4j.Slf4j;

import java.util.*;

@Slf4j
public class KafkaCollector extends Collector {
    private static final String kafkaTopicOffset = "kafka_topic_offset";
    private static final String kafkaTopicBrokerOffset = "kafka_topic_broker_offset";
    private static final String kafkaConsumerTopicOffset = "kafka_consumer_topic_offset";
    private static final String kafkaConsumerBrokerTopicOffset = "kafka_consumer_broker_topic_offset";
    private static final String kafkaConsumerTopicLag = "kafka_consumer_topic_lag";
    private static final String kafkaConsumerBrokerTopicLag = "kafka_consumer_broker_topic_lag";
    //分隔符
    private static final String DELIMITER = "@&@";
    @Override
    public List<MetricFamilySamples> collect() {
        try{
            return getKafkaMetrics();
        } catch (Exception e) {
            log.error("Failed to collect kafka metrics", e);
            return new ArrayList<>();
        }
    }

    private List<MetricFamilySamples> getKafkaMetrics() {
        List<MetricFamilySamples> kafkaMetricFamilySamples = new ArrayList<>();
        //查询所有topic的offset
        ArrayList<TopicPartitionOffsetMetric> topicPartitionOffsetMetrics = TopicPartitionOffset.get(Config.BROKER_LIST);
        //查询所有消费者组的offset和lag
        ArrayList<ConsumerTopicPartitionOffsetMetric> consumerTopicPartitionOffsetMetrics = ConsumerTopicPartitionOffset.get(Config.BROKER_LIST);
        //三个partition维度的metric
        ArrayList<MetricFamilySamples.Sample> partitionOffsetSamples = new ArrayList<>();
        ArrayList<MetricFamilySamples.Sample> consumerPartitionOffsetSamples = new ArrayList<>();
        ArrayList<MetricFamilySamples.Sample> consumerPartitionLagSamples = new ArrayList<>();
        //三个broker维度的metric
        ArrayList<MetricFamilySamples.Sample> brokerOffsetSamples = new ArrayList<>();
        ArrayList<MetricFamilySamples.Sample> consumerBrokerOffsetSamples = new ArrayList<>();
        ArrayList<MetricFamilySamples.Sample> consumerBrokerLagSamples = new ArrayList<>();
        //三个topic维度的metric
        ArrayList<MetricFamilySamples.Sample> topicOffsetSamples = new ArrayList<>();
        ArrayList<MetricFamilySamples.Sample> consumerTopicOffsetSamples = new ArrayList<>();
        ArrayList<MetricFamilySamples.Sample> consumerTopicLagSamples = new ArrayList<>();
        //根据topic和partition所拼接的字符串，查找对应的TopicPartitionOffsetMetric
        Map<String, TopicPartitionOffsetMetric> topicPartitionOffsetMetricMap = new HashMap<>();
        topicPartitionOffsetMetrics.forEach(metric -> topicPartitionOffsetMetricMap.put(metric.getTopic() + "-" + metric.getPartition(), metric));
        //根据topic和partition所拼接的字符串，查找对应的ConsumerTopicPartitionOffsetMetric
        Map<String, ConsumerTopicPartitionOffsetMetric> consumerTopicPartitionOffsetMetricMap = new HashMap<>();
        consumerTopicPartitionOffsetMetrics.forEach(metric -> consumerTopicPartitionOffsetMetricMap.put(metric.getTopic() + "-" + metric.getPartition(), metric));
        //开始生成metric
        //一个嵌套Map，用来计算broker上某个topic的offset，key为topic，value为broker和对应的offset
        Map<String, Map<String, Long>> brokerOffsetMap = new HashMap<>();
        //两个个嵌套Map，分别用来计算broker上某个topic的消费offset和lag的总和，key为consumerGroup和topic的组合，value为broker和对应的offset、消费offset和lag
        Map<String, Map<String, Long>> consumerBrokerOffsetMap = new HashMap<>();
        Map<String, Map<String, Long>> consumerBrokerLagMap = new HashMap<>();
        for (TopicPartitionOffsetMetric metric : topicPartitionOffsetMetrics) {
            //根据topic和partition所拼接的字符串，查找对应的ConsumerTopicPartitionOffsetMetric
            ConsumerTopicPartitionOffsetMetric consumerTopicPartitionOffsetMetric = consumerTopicPartitionOffsetMetricMap.get(metric.getTopic() + "-" + metric.getPartition());
            //如果找到了，将endOffset赋值给metric
            if (consumerTopicPartitionOffsetMetric != null) {
                metric.setOffset(consumerTopicPartitionOffsetMetric.getLogEndOffset());
            }
            //kafka_topic_partition_offset
            partitionOffsetSamples.add(new MetricFamilySamples.Sample(
                    TopicPartitionOffsetMetric.METRIC_NAME,
                    List.of(TopicPartitionOffsetMetric.HEADERS),
                    Arrays.asList(metric.toArray()),
                    metric.getOffset()
            ));
            String leader = metric.getLeader();
            String topic = metric.getTopic();
            //broker维度求和
            Map<String, Long> brokerOffset = brokerOffsetMap.computeIfAbsent(topic, k -> new HashMap<>());
            brokerOffset.put(leader, brokerOffset.getOrDefault(leader, 0L) + metric.getOffset());
        }
        for (ConsumerTopicPartitionOffsetMetric metric : consumerTopicPartitionOffsetMetrics) {
            //根据topic和partition所拼接的字符串，查找对应的TopicPartitionOffsetMetric
            TopicPartitionOffsetMetric topicPartitionOffsetMetric = topicPartitionOffsetMetricMap.get(metric.getTopic() + "-" + metric.getPartition());
            //如果找到了，将leader赋值给metric
            if (topicPartitionOffsetMetric != null) {
                metric.setLeader(topicPartitionOffsetMetric.getLeader());
            }
            consumerPartitionOffsetSamples.add(new MetricFamilySamples.Sample(
                    ConsumerTopicPartitionOffsetMetric.METRIC_NAME_OFFSET,
                    List.of(ConsumerTopicPartitionOffsetMetric.HEADERS),
                    Arrays.asList(metric.toArray()),
                    metric.getOffset()
            ));
            consumerPartitionLagSamples.add(new MetricFamilySamples.Sample(
                    ConsumerTopicPartitionOffsetMetric.METRIC_NAME_LAG,
                    List.of(ConsumerTopicPartitionOffsetMetric.HEADERS),
                    Arrays.asList(metric.toArray()),
                    metric.getLag()
            ));
            String key = metric.getConsumerGroup() + DELIMITER + metric.getTopic();
            String leader = metric.getLeader();
            //broker维度求和，消费offset
            Map<String, Long> consumerBrokerOffset = consumerBrokerOffsetMap.computeIfAbsent(key, k -> new HashMap<>());
            consumerBrokerOffset.put(leader, consumerBrokerOffset.getOrDefault(leader, 0L) + metric.getOffset());
            //broker维度求和，消费lag
            Map<String, Long> consumerBrokerLag = consumerBrokerLagMap.computeIfAbsent(key, k -> new HashMap<>());
            consumerBrokerLag.put(leader, consumerBrokerLag.getOrDefault(leader, 0L) + metric.getLag());
        }
        brokerOffsetMap.forEach((topic, brokerOffset) -> {
            long totalOffset = brokerOffset.values().stream().mapToLong(Long::longValue).sum();
            topicOffsetSamples.add(new MetricFamilySamples.Sample(
                    kafkaTopicOffset,
                    List.of("topic"),
                    List.of(topic),
                    totalOffset
            ));
            brokerOffset.forEach((broker, offset) -> brokerOffsetSamples.add(new MetricFamilySamples.Sample(
                    kafkaTopicBrokerOffset,
                    List.of("topic", "broker"),
                    Arrays.asList(topic, broker),
                    offset
            )));
        });
        consumerBrokerOffsetMap.forEach((key, consumerBrokerOffset) -> {
            long totalOffset = consumerBrokerOffset.values().stream().mapToLong(Long::longValue).sum();
            String consumerGroup = key.split(DELIMITER)[0];
            String topic = key.split(DELIMITER)[1];
            consumerTopicOffsetSamples.add(new MetricFamilySamples.Sample(
                    kafkaConsumerTopicOffset,
                    List.of("consumer_group", "topic"),
                    Arrays.asList(consumerGroup, topic),
                    totalOffset
            ));
            consumerBrokerOffset.forEach((broker, offset) -> consumerBrokerOffsetSamples.add(new MetricFamilySamples.Sample(
                    kafkaConsumerBrokerTopicOffset,
                    List.of("consumer_group", "topic", "broker"),
                    Arrays.asList(consumerGroup, topic, broker),
                    offset
            )));
        });
        consumerBrokerLagMap.forEach((key, consumerBrokerLag) -> {
            long totalLag = consumerBrokerLag.values().stream().mapToLong(Long::longValue).sum();
            String consumerGroup = key.split(DELIMITER)[0];
            String topic = key.split(DELIMITER)[1];
            consumerTopicLagSamples.add(new MetricFamilySamples.Sample(
                    kafkaConsumerTopicLag,
                    List.of("consumer_group", "topic"),
                    Arrays.asList(consumerGroup, topic),
                    totalLag
            ));
            consumerBrokerLag.forEach((broker, lag) -> consumerBrokerLagSamples.add(new MetricFamilySamples.Sample(
                    kafkaConsumerBrokerTopicLag,
                    List.of("consumer_group", "topic", "broker"),
                    Arrays.asList(consumerGroup, topic, broker),
                    lag
            )));
        });
        //offset
        kafkaMetricFamilySamples.add(new MetricFamilySamples(
                TopicPartitionOffsetMetric.METRIC_NAME,
                Type.GAUGE,
                "help",
                partitionOffsetSamples));
        kafkaMetricFamilySamples.add(new MetricFamilySamples(
                kafkaTopicBrokerOffset,
                Type.GAUGE,
                "help",
                brokerOffsetSamples));
        kafkaMetricFamilySamples.add(new MetricFamilySamples(
                kafkaTopicOffset,
                Type.GAUGE,
                "help",
                topicOffsetSamples));
        //consumer_offset
        kafkaMetricFamilySamples.add(new MetricFamilySamples(
                ConsumerTopicPartitionOffsetMetric.METRIC_NAME_OFFSET,
                Type.GAUGE,
                "help",
                consumerPartitionOffsetSamples));
        kafkaMetricFamilySamples.add(new MetricFamilySamples(
                kafkaConsumerBrokerTopicOffset,
                Type.GAUGE,
                "help",
                consumerBrokerOffsetSamples));
        kafkaMetricFamilySamples.add(new MetricFamilySamples(
                kafkaConsumerTopicOffset,
                Type.GAUGE,
                "help",
                consumerTopicOffsetSamples));
        //consumer_lag
        kafkaMetricFamilySamples.add(new MetricFamilySamples(
                ConsumerTopicPartitionOffsetMetric.METRIC_NAME_LAG,
                Type.GAUGE,
                "help",
                consumerPartitionLagSamples));
        kafkaMetricFamilySamples.add(new MetricFamilySamples(
                kafkaConsumerBrokerTopicLag,
                Type.GAUGE,
                "help",
                consumerBrokerLagSamples));
        kafkaMetricFamilySamples.add(new MetricFamilySamples(
                kafkaConsumerTopicLag,
                Type.GAUGE,
                "help",
                consumerTopicLagSamples));
        return kafkaMetricFamilySamples;
    }

}