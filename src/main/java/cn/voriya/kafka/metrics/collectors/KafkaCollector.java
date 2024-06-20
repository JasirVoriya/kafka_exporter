package cn.voriya.kafka.metrics.collectors;

import cn.voriya.kafka.metrics.config.Config;
import cn.voriya.kafka.metrics.config.ConfigCluster;
import cn.voriya.kafka.metrics.entity.TopicConsumerResponse;
import cn.voriya.kafka.metrics.entity.TopicProducerResponse;
import cn.voriya.kafka.metrics.metrics.*;
import cn.voriya.kafka.metrics.request.TopicConsumerOffset;
import cn.voriya.kafka.metrics.request.TopicProducerOffset;
import cn.voriya.kafka.metrics.thread.ThreadPool;
import io.prometheus.client.Collector;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.time.StopWatch;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

@Log4j2
public class KafkaCollector extends Collector {
    //分隔符
    private static final String DELIMITER = "@&@";
    @Override
    public List<MetricFamilySamples> collect() {
        Map<String, MetricFamilySamples> samples = getAllClusterMetrics();
        return new ArrayList<>(samples.values());
    }

    private Map<String, MetricFamilySamples> getAllClusterMetrics() {
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
                    Map<String, MetricFamilySamples> kafkaMetrics = getClusterMetrics(configCluster);
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
        return samples;
    }

    private Map<String, MetricFamilySamples> getClusterMetrics(ConfigCluster configCluster) {
        //查询所有topic的offset
        ArrayList<TopicProducerResponse> topicProducerResponses = TopicProducerOffset.get(configCluster);
        //查询所有消费者组的offset和lag
        ArrayList<TopicConsumerResponse> topicConsumerResponses = TopicConsumerOffset.get(configCluster);
        //三个metric
        ProducerOffsetMetric producerOffsetMetric = new ProducerOffsetMetric();
        ConsumerOffsetMetric consumerOffsetMetric = new ConsumerOffsetMetric();
        ConsumerLagMetric consumerLagMetric = new ConsumerLagMetric();
        ConsumerGroupOffsetMetric consumerGroupOffsetMetric = new ConsumerGroupOffsetMetric();
        ConsumerGroupLagMetric consumerGroupLagMetric = new ConsumerGroupLagMetric();
        //根据topic和partition所拼接的字符串，查找对应的TopicProducerOffsetMetric
        Map<String, TopicProducerResponse> producerOffsetMetricMap = new HashMap<>();
        topicProducerResponses.forEach(metric -> producerOffsetMetricMap.put(metric.getTopic() + DELIMITER + metric.getPartition(), metric));
        //根据topic和partition所拼接的字符串，查找对应的ConsumerTopicPartitionOffsetMetric
        Map<String, TopicConsumerResponse> consumerOffsetMetricMap = new HashMap<>();
        topicConsumerResponses.forEach(metric -> consumerOffsetMetricMap.put(metric.getTopic() + DELIMITER + metric.getPartition(), metric));
        //开始生成metric
        for (TopicProducerResponse res : topicProducerResponses) {
            //根据topic和partition所拼接的字符串，查找对应的ConsumerTopicPartitionOffsetMetric
            TopicConsumerResponse topicConsumerResponse = consumerOffsetMetricMap.get(res.getTopic() + DELIMITER + res.getPartition());
            //如果找到了，将endOffset赋值给metric
            if (topicConsumerResponse != null) {
                res.setOffset(topicConsumerResponse.getLogEndOffset());
            }
            producerOffsetMetric.add(res, configCluster);
        }
        for (TopicConsumerResponse res : topicConsumerResponses) {
            //根据topic和partition所拼接的字符串，查找对应的TopicPartitionOffsetMetric
            TopicProducerResponse topicProducerResponse = producerOffsetMetricMap.get(res.getTopic() + DELIMITER + res.getPartition());
            //如果找到了，将leader赋值给metric
            if (topicProducerResponse != null) {
                res.setLeader(topicProducerResponse.getLeader());
            }
            consumerOffsetMetric.add(res, configCluster);
            consumerLagMetric.add(res, configCluster);
            consumerGroupLagMetric.add(res, configCluster);
            consumerGroupOffsetMetric.add(res, configCluster);
        }
        //offset
        //consumer_offset
        //consumer_lag
        Map<String, MetricFamilySamples> map = new HashMap<>();
        map.put(producerOffsetMetric.name, producerOffsetMetric);
        map.put(consumerOffsetMetric.name, consumerOffsetMetric);
        map.put(consumerLagMetric.name, consumerLagMetric);
        map.put(consumerGroupOffsetMetric.name, consumerGroupOffsetMetric);
        map.put(consumerGroupLagMetric.name, consumerGroupLagMetric);
        return map;
    }
}