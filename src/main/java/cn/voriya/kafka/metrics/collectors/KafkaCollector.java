package cn.voriya.kafka.metrics.collectors;

import cn.voriya.kafka.metrics.config.Config;
import cn.voriya.kafka.metrics.config.ConfigCluster;
import cn.voriya.kafka.metrics.entity.TopicConsumerOffsetMetric;
import cn.voriya.kafka.metrics.entity.TopicProducerOffsetMetric;
import cn.voriya.kafka.metrics.request.TopicConsumerOffset;
import cn.voriya.kafka.metrics.request.TopicPartitionOffset;
import cn.voriya.kafka.metrics.thread.ThreadPool;
import io.prometheus.client.Collector;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.time.StopWatch;

import java.util.*;
import java.util.concurrent.Future;

@Log4j2
public class KafkaCollector extends Collector {
    public static final String kafkaTopicProducerOffset = "kafka_topic_producer_offset";
    public static final String kafkaTopicConsumerOffset = "kafka_topic_consumer_offset";
    public static final String kafkaTopicConsumerLag = "kafka_topic_consumer_lag";
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
                futures.add(ThreadPool.CLUSTER_POOL.submit(() -> {
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
        ArrayList<TopicProducerOffsetMetric> topicProducerOffsetMetrics = TopicPartitionOffset.get(configCluster);
        //查询所有消费者组的offset和lag
        ArrayList<TopicConsumerOffsetMetric> topicConsumerOffsetMetrics = TopicConsumerOffset.get(configCluster);
        //三个metric
        ArrayList<MetricFamilySamples.Sample> producerOffsetSamples = new ArrayList<>();
        ArrayList<MetricFamilySamples.Sample> consumerOffsetSamples = new ArrayList<>();
        ArrayList<MetricFamilySamples.Sample> consumerLagSamples = new ArrayList<>();
        //根据topic和partition所拼接的字符串，查找对应的TopicProducerOffsetMetric
        Map<String, TopicProducerOffsetMetric> producerOffsetMetricMap = new HashMap<>();
        topicProducerOffsetMetrics.forEach(metric -> producerOffsetMetricMap.put(metric.getTopic() + DELIMITER + metric.getPartition(), metric));
        //根据topic和partition所拼接的字符串，查找对应的ConsumerTopicPartitionOffsetMetric
        Map<String, TopicConsumerOffsetMetric> consumerOffsetMetricMap = new HashMap<>();
        topicConsumerOffsetMetrics.forEach(metric -> consumerOffsetMetricMap.put(metric.getTopic() + DELIMITER + metric.getPartition(), metric));
        //开始生成metric
        for (TopicProducerOffsetMetric metric : topicProducerOffsetMetrics) {
            //根据topic和partition所拼接的字符串，查找对应的ConsumerTopicPartitionOffsetMetric
            TopicConsumerOffsetMetric topicConsumerOffsetMetric = consumerOffsetMetricMap.get(metric.getTopic() + DELIMITER + metric.getPartition());
            //如果找到了，将endOffset赋值给metric
            if (topicConsumerOffsetMetric != null) {
                metric.setOffset(topicConsumerOffsetMetric.getLogEndOffset());
            }
            //kafka_topic_partition_offset
            producerOffsetSamples.add(new MetricFamilySamples.Sample(
                    kafkaTopicProducerOffset,
                    List.of("cluster", "topic", "partition", "leader"),
                    Arrays.asList(configCluster.getName(), metric.getTopic(), String.valueOf(metric.getPartition()), metric.getLeader()),
                    metric.getOffset()
            ));
        }
        for (TopicConsumerOffsetMetric metric : topicConsumerOffsetMetrics) {
            //根据topic和partition所拼接的字符串，查找对应的TopicPartitionOffsetMetric
            TopicProducerOffsetMetric topicProducerOffsetMetric = producerOffsetMetricMap.get(metric.getTopic() + DELIMITER + metric.getPartition());
            //如果找到了，将leader赋值给metric
            if (topicProducerOffsetMetric != null) {
                metric.setLeader(topicProducerOffsetMetric.getLeader());
            }
            consumerOffsetSamples.add(new MetricFamilySamples.Sample(
                    kafkaTopicConsumerOffset,
                    List.of("cluster", "consumer_group", "topic", "partition", "leader", "coordinator", "consumer_id", "host", "client_id"),
                    Arrays.asList(configCluster.getName(), metric.getConsumerGroup(), metric.getTopic(), String.valueOf(metric.getPartition()), metric.getLeader(), metric.getCoordinator(), metric.getConsumerId(), metric.getHost(), metric.getClientId()),
                    metric.getOffset()
            ));
            consumerLagSamples.add(new MetricFamilySamples.Sample(
                    kafkaTopicConsumerLag,
                    List.of("cluster", "consumer_group", "topic", "partition", "leader", "coordinator", "consumer_id", "host", "client_id"),
                    Arrays.asList(configCluster.getName(), metric.getConsumerGroup(), metric.getTopic(), String.valueOf(metric.getPartition()), metric.getLeader(), metric.getCoordinator(), metric.getConsumerId(), metric.getHost(), metric.getClientId()),
                    metric.getLag()
            ));
        }
        //offset
        //consumer_offset
        //consumer_lag
        Map<String, MetricFamilySamples> map = new HashMap<>();
        map.put(kafkaTopicProducerOffset, new MetricFamilySamples(kafkaTopicProducerOffset, Type.GAUGE, "help", producerOffsetSamples));
        map.put(kafkaTopicConsumerOffset, new MetricFamilySamples(kafkaTopicConsumerOffset, Type.GAUGE, "help", consumerOffsetSamples));
        map.put(kafkaTopicConsumerLag, new MetricFamilySamples(kafkaTopicConsumerLag, Type.GAUGE, "help", consumerLagSamples));
        return map;
    }
}