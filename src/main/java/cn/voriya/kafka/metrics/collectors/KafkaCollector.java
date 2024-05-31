package cn.voriya.kafka.metrics.collectors;

import cn.voriya.kafka.metrics.config.Config;
import cn.voriya.kafka.metrics.config.ConfigCluster;
import cn.voriya.kafka.metrics.entity.ConsumerTopicPartitionOffsetMetric;
import cn.voriya.kafka.metrics.entity.TopicPartitionOffsetMetric;
import cn.voriya.kafka.metrics.request.ConsumerTopicPartitionOffset;
import cn.voriya.kafka.metrics.request.TopicPartitionOffset;
import cn.voriya.kafka.metrics.thread.ThreadPool;
import io.prometheus.client.Collector;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.time.StopWatch;

import java.util.*;
import java.util.concurrent.Future;

@Log4j2
public class KafkaCollector extends Collector {
    private static final String kafkaTopicOffset = "kafka_topic_offset";
    private static final String kafkaTopicBrokerOffset = "kafka_topic_broker_offset";
    private static final String kafkaConsumerTopicOffset = "kafka_consumer_topic_offset";
    private static final String kafkaConsumerBrokerTopicOffset = "kafka_consumer_broker_topic_offset";
    private static final String kafkaConsumerTopicLag = "kafka_consumer_topic_lag";
    private static final String kafkaConsumerBrokerTopicLag = "kafka_consumer_broker_topic_lag";
    public static final String kafkaTopicPartitionOffset = "kafka_topic_partition_offset";
    public static final String kafkaConsumerTopicPartitionOffset = "kafka_consumer_topic_partition_offset";
    public static final String kafkaConsumerTopicPartitionLag = "kafka_consumer_topic_partition_lag";
    //分隔符
    private static final String DELIMITER = "@&@";
    @Override
    public List<MetricFamilySamples> collect() {
        Map<String, MetricFamilySamples> samples = new HashMap<>();
        StopWatch totalStopWatch = StopWatch.createStarted();
        try{
            Config config = Config.getInstance();
            List<Future<Map<String, MetricFamilySamples>>> futures = new ArrayList<>();
            //每个集群提交到一个线程里面去采集
            for (ConfigCluster configCluster : config.getCluster()) {
                futures.add(ThreadPool.CONSUMERS_POOL.submit(() -> {
                    log.info("Start to collect kafka metrics, cluster: [{}]", configCluster.getName());
                    StopWatch clusterStopWatch = StopWatch.createStarted();
                    Map<String, MetricFamilySamples> kafkaMetrics = getKafkaMetrics(configCluster);
                    log.info("Finish to collect kafka metrics, cluster: [{}], time: {}ms", configCluster.getName(), clusterStopWatch.getTime());
                    return kafkaMetrics;
                }));
            }
            //获取每个集群的采集结果
            for (Future<Map<String, MetricFamilySamples>> future : futures) {
                Map<String, MetricFamilySamples> kafkaMetrics = future.get();
                for (Map.Entry<String, MetricFamilySamples> entry : kafkaMetrics.entrySet()) {
                    if (!samples.containsKey(entry.getKey())) {
                        samples.put(entry.getKey(), entry.getValue());
                        continue;
                    }
                    samples.get(entry.getKey()).samples.addAll(entry.getValue().samples);
                }
            }
        } catch (Exception e) {
            log.error("Failed to collect kafka metrics, total time: {}ms", totalStopWatch.getTime(), e);
        }
        log.info("Finish to collect all kafka metrics, total time: {}ms", totalStopWatch.getTime());
        return new ArrayList<>(samples.values());
    }

    private Map<String, MetricFamilySamples> getKafkaMetrics(ConfigCluster configCluster) {
        //查询所有topic的offset
        ArrayList<TopicPartitionOffsetMetric> topicPartitionOffsetMetrics = TopicPartitionOffset.get(String.join(",", configCluster.getBrokers()));
        //查询所有消费者组的offset和lag
        ArrayList<ConsumerTopicPartitionOffsetMetric> consumerTopicPartitionOffsetMetrics = ConsumerTopicPartitionOffset.get(String.join(",", configCluster.getBrokers()));
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
        topicPartitionOffsetMetrics.forEach(metric -> topicPartitionOffsetMetricMap.put(metric.getTopic() + DELIMITER + metric.getPartition(), metric));
        //根据topic和partition所拼接的字符串，查找对应的ConsumerTopicPartitionOffsetMetric
        Map<String, ConsumerTopicPartitionOffsetMetric> consumerTopicPartitionOffsetMetricMap = new HashMap<>();
        consumerTopicPartitionOffsetMetrics.forEach(metric -> consumerTopicPartitionOffsetMetricMap.put(metric.getTopic() + DELIMITER + metric.getPartition(), metric));
        //开始生成metric
        //一个嵌套Map，用来计算broker上某个topic的offset，key为topic，value为broker和对应的offset
        Map<String, Map<String, Long>> brokerOffsetMap = new HashMap<>();
        //两个个嵌套Map，分别用来计算broker上某个topic的消费offset和lag的总和，key为consumerGroup和topic的组合，value为broker和对应的offset、消费offset和lag
        Map<String, Map<String, Long>> consumerBrokerOffsetMap = new HashMap<>();
        Map<String, Map<String, Long>> consumerBrokerLagMap = new HashMap<>();
        for (TopicPartitionOffsetMetric metric : topicPartitionOffsetMetrics) {
            //根据topic和partition所拼接的字符串，查找对应的ConsumerTopicPartitionOffsetMetric
            ConsumerTopicPartitionOffsetMetric consumerTopicPartitionOffsetMetric = consumerTopicPartitionOffsetMetricMap.get(metric.getTopic() + DELIMITER + metric.getPartition());
            //如果找到了，将endOffset赋值给metric
            if (consumerTopicPartitionOffsetMetric != null) {
                metric.setOffset(consumerTopicPartitionOffsetMetric.getLogEndOffset());
            }
            //kafka_topic_partition_offset
            partitionOffsetSamples.add(new MetricFamilySamples.Sample(
                    kafkaTopicPartitionOffset,
                    List.of("cluster", "topic", "partition", "leader"),
                    Arrays.asList(configCluster.getName(), metric.getTopic(), String.valueOf(metric.getPartition()), metric.getLeader()),
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
            TopicPartitionOffsetMetric topicPartitionOffsetMetric = topicPartitionOffsetMetricMap.get(metric.getTopic() + DELIMITER + metric.getPartition());
            //如果找到了，将leader赋值给metric
            if (topicPartitionOffsetMetric != null) {
                metric.setLeader(topicPartitionOffsetMetric.getLeader());
            }
            consumerPartitionOffsetSamples.add(new MetricFamilySamples.Sample(
                    kafkaConsumerTopicPartitionOffset,
                    List.of("cluster", "consumer_group", "topic", "partition", "leader", "coordinator", "consumer_id", "host", "client_id"),
                    Arrays.asList(configCluster.getName(), metric.getConsumerGroup(), metric.getTopic(), String.valueOf(metric.getPartition()), metric.getLeader(), metric.getCoordinator(), metric.getConsumerId(), metric.getHost(), metric.getClientId()),
                    metric.getOffset()
            ));
            consumerPartitionLagSamples.add(new MetricFamilySamples.Sample(
                    kafkaConsumerTopicPartitionLag,
                    List.of("cluster", "consumer_group", "topic", "partition", "leader", "coordinator", "consumer_id", "host", "client_id"),
                    Arrays.asList(configCluster.getName(), metric.getConsumerGroup(), metric.getTopic(), String.valueOf(metric.getPartition()), metric.getLeader(), metric.getCoordinator(), metric.getConsumerId(), metric.getHost(), metric.getClientId()),
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
                    List.of("cluster", "topic"),
                    List.of(configCluster.getName(), topic),
                    totalOffset
            ));
            brokerOffset.forEach((broker, offset) -> brokerOffsetSamples.add(new MetricFamilySamples.Sample(
                    kafkaTopicBrokerOffset,
                    List.of("cluster", "topic", "broker"),
                    Arrays.asList(configCluster.getName(), topic, broker),
                    offset
            )));
        });
        consumerBrokerOffsetMap.forEach((key, consumerBrokerOffset) -> {
            long totalOffset = consumerBrokerOffset.values().stream().mapToLong(Long::longValue).sum();
            String consumerGroup = key.split(DELIMITER)[0];
            String topic = key.split(DELIMITER)[1];
            consumerTopicOffsetSamples.add(new MetricFamilySamples.Sample(
                    kafkaConsumerTopicOffset,
                    List.of("cluster", "consumer_group", "topic"),
                    Arrays.asList(configCluster.getName(), consumerGroup, topic),
                    totalOffset
            ));
            consumerBrokerOffset.forEach((broker, offset) -> consumerBrokerOffsetSamples.add(new MetricFamilySamples.Sample(
                    kafkaConsumerBrokerTopicOffset,
                    List.of("cluster", "consumer_group", "topic", "broker"),
                    Arrays.asList(configCluster.getName(), consumerGroup, topic, broker),
                    offset
            )));
        });
        consumerBrokerLagMap.forEach((key, consumerBrokerLag) -> {
            long totalLag = consumerBrokerLag.values().stream().mapToLong(Long::longValue).sum();
            String consumerGroup = key.split(DELIMITER)[0];
            String topic = key.split(DELIMITER)[1];
            consumerTopicLagSamples.add(new MetricFamilySamples.Sample(
                    kafkaConsumerTopicLag,
                    List.of("cluster", "consumer_group", "topic"),
                    Arrays.asList(configCluster.getName(), consumerGroup, topic),
                    totalLag
            ));
            consumerBrokerLag.forEach((broker, lag) -> consumerBrokerLagSamples.add(new MetricFamilySamples.Sample(
                    kafkaConsumerBrokerTopicLag,
                    List.of("cluster", "consumer_group", "topic", "broker"),
                    Arrays.asList(configCluster.getName(), consumerGroup, topic, broker),
                    lag
            )));
        });
        //offset
        //consumer_offset
        //consumer_lag
        Map<String, MetricFamilySamples> map = new HashMap<>();
        map.put(kafkaTopicPartitionOffset, new MetricFamilySamples(kafkaTopicPartitionOffset, Type.GAUGE, "help", partitionOffsetSamples));
        map.put(kafkaTopicBrokerOffset, new MetricFamilySamples(kafkaTopicBrokerOffset, Type.GAUGE, "help", brokerOffsetSamples));
        map.put(kafkaTopicOffset, new MetricFamilySamples(kafkaTopicOffset, Type.GAUGE, "help", topicOffsetSamples));
        map.put(kafkaConsumerTopicPartitionOffset, new MetricFamilySamples(kafkaConsumerTopicPartitionOffset, Type.GAUGE, "help", consumerPartitionOffsetSamples));
        map.put(kafkaConsumerBrokerTopicOffset, new MetricFamilySamples(kafkaConsumerBrokerTopicOffset, Type.GAUGE, "help", consumerBrokerOffsetSamples));
        map.put(kafkaConsumerTopicOffset, new MetricFamilySamples(kafkaConsumerTopicOffset, Type.GAUGE, "help", consumerTopicOffsetSamples));
        map.put(kafkaConsumerTopicPartitionLag, new MetricFamilySamples(kafkaConsumerTopicPartitionLag, Type.GAUGE, "help", consumerPartitionLagSamples));
        map.put(kafkaConsumerBrokerTopicLag, new MetricFamilySamples(kafkaConsumerBrokerTopicLag, Type.GAUGE, "help", consumerBrokerLagSamples));
        map.put(kafkaConsumerTopicLag, new MetricFamilySamples(kafkaConsumerTopicLag, Type.GAUGE, "help", consumerTopicLagSamples));
        return map;
    }
}