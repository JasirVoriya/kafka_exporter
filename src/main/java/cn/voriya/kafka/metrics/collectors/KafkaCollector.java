package cn.voriya.kafka.metrics.collectors;

import cn.voriya.kafka.metrics.config.Config;
import cn.voriya.kafka.metrics.config.ConfigCluster;
import cn.voriya.kafka.metrics.entity.TopicConsumerEntity;
import cn.voriya.kafka.metrics.entity.TopicGroupEntity;
import cn.voriya.kafka.metrics.entity.TopicProducerEntity;
import cn.voriya.kafka.metrics.metrics.*;
import cn.voriya.kafka.metrics.request.TopicConsumerOffset;
import cn.voriya.kafka.metrics.request.TopicProducerOffset;
import cn.voriya.kafka.metrics.thread.ThreadPool;
import io.prometheus.client.Collector;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.time.StopWatch;

import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Log4j2
public class KafkaCollector extends Collector {
    //分隔符
    private static final String DELIMITER = "@&@";
    private static final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private Map<String, MetricFamilySamples> cache = new HashMap<>();
    private final Object lock = new Object();

    public KafkaCollector() {
        this.updateCache();
        scheduler.scheduleAtFixedRate(this::updateCache, 0, Config.getInstance().getInterval(), TimeUnit.SECONDS);
    }

    private void updateCache() {
        synchronized (lock) {
            this.cache = getAllClusterMetrics();
            lock.notifyAll();
        }
    }

    @Override
    public List<MetricFamilySamples> collect() {
        synchronized (lock) {
            return new ArrayList<>(cache.values());
        }
    }

    private Map<String, MetricFamilySamples> getAllClusterMetrics() {
        Map<String, MetricFamilySamples> samples = new HashMap<>();
        StopWatch totalStopWatch = StopWatch.createStarted();
        try{
            Config config = Config.getInstance();
            List<Future<Map<String, MetricFamilySamples>>> futures = new LinkedList<>();
            //每个集群提交到一个线程里面去采集
            for (ConfigCluster configCluster : config.getCluster()) {
                futures.add(ThreadPool.CLUSTER_POOL.submit(() -> {
                    log.info("Start to collect kafka metrics, cluster: [{}]", configCluster.getName());
                    StopWatch clusterStopWatch = StopWatch.createStarted();
                    Map<String, MetricFamilySamples> clusterMetrics = getClusterMetrics(configCluster);
                    ExporterClusterTimeMetric exporterClusterTimeMetric = new ExporterClusterTimeMetric();
                    exporterClusterTimeMetric.add(configCluster.getName(), clusterStopWatch.getTime());
                    clusterMetrics.put(exporterClusterTimeMetric.name, exporterClusterTimeMetric);
                    log.info("Finish to collect kafka metrics, cluster: [{}], time: {}ms", configCluster.getName(), clusterStopWatch.getTime());
                    return clusterMetrics;
                }));
            }
            //获取每个集群的采集结果
            for (Future<Map<String, MetricFamilySamples>> future : futures) {
                Map<String, MetricFamilySamples> clusterMetrics = future.get();
                for (Map.Entry<String, MetricFamilySamples> entry : clusterMetrics.entrySet()) {
                    if (!samples.containsKey(entry.getKey())) {
                        samples.put(entry.getKey(), entry.getValue());
                        continue;
                    }
                    samples.get(entry.getKey()).samples.addAll(entry.getValue().samples);
                }
            }
            log.info("Finish to collect all kafka metrics, total time: {}ms", totalStopWatch.getTime());
        } catch (Exception e) {
            log.error("Failed to collect kafka metrics, total time: {}ms", totalStopWatch.getTime(), e);
        }
        ExporterTotalTimeMetric exporterTotalTimeMetric = new ExporterTotalTimeMetric();
        exporterTotalTimeMetric.add(totalStopWatch.getTime());
        samples.put(exporterTotalTimeMetric.name, exporterTotalTimeMetric);
        return samples;
    }

    private Map<String, MetricFamilySamples> getClusterMetrics(ConfigCluster configCluster) {
        //查询所有topic的offset
        List<TopicProducerEntity> topicProducers = TopicProducerOffset.get(configCluster);
        //查询所有消费者组的offset和lag
        List<TopicGroupEntity> topicGroups = TopicConsumerOffset.get(configCluster);
        //time metrics
        ExporterGroupTimeMetric exporterGroupTimeMetric = new ExporterGroupTimeMetric();
        //metrics
        ProducerOffsetMetric producerOffsetMetric = new ProducerOffsetMetric();
        ConsumerOffsetMetric consumerOffsetMetric = new ConsumerOffsetMetric();
        ConsumerLagMetric consumerLagMetric = new ConsumerLagMetric();
        ConsumerGroupOffsetMetric consumerGroupOffsetMetric = new ConsumerGroupOffsetMetric();
        ConsumerGroupLagMetric consumerGroupLagMetric = new ConsumerGroupLagMetric();
        //start make metrics
        List<TopicConsumerEntity> topicConsumers = new LinkedList<>();
        topicGroups.forEach(group -> {
            topicConsumers.addAll(group.getConsumers());
            exporterGroupTimeMetric.add(group);
        });
        //根据topic和partition所拼接的字符串，查找对应的TopicProducerOffsetMetric
        Map<String, TopicProducerEntity> producerOffsetMetricMap = new HashMap<>();
        topicProducers.forEach(metric -> producerOffsetMetricMap.put(metric.getTopic() + DELIMITER + metric.getPartition(), metric));
        //根据topic和partition所拼接的字符串，查找对应的ConsumerTopicPartitionOffsetMetric
        Map<String, TopicConsumerEntity> consumerOffsetMetricMap = new HashMap<>();
        topicConsumers.forEach(metric -> consumerOffsetMetricMap.put(metric.getTopic() + DELIMITER + metric.getPartition(), metric));
        //开始生成metric
        for (TopicProducerEntity res : topicProducers) {
            //根据topic和partition所拼接的字符串，查找对应的ConsumerTopicPartitionOffsetMetric
            TopicConsumerEntity topicConsumerEntity = consumerOffsetMetricMap.get(res.getTopic() + DELIMITER + res.getPartition());
            //如果找到了，将endOffset赋值给metric
            if (topicConsumerEntity != null) {
                res.setOffset(topicConsumerEntity.getLogEndOffset());
            }
            producerOffsetMetric.add(res, configCluster);
        }
        for (TopicConsumerEntity res : topicConsumers) {
            //根据topic和partition所拼接的字符串，查找对应的TopicPartitionOffsetMetric
            TopicProducerEntity topicProducerEntity = producerOffsetMetricMap.get(res.getTopic() + DELIMITER + res.getPartition());
            //如果找到了，将leader赋值给metric
            if (topicProducerEntity != null) {
                res.setLeader(topicProducerEntity.getLeader());
            }
            consumerOffsetMetric.add(res, configCluster);
            consumerLagMetric.add(res, configCluster);
            consumerGroupLagMetric.add(res, configCluster);
            consumerGroupOffsetMetric.add(res, configCluster);
        }
        return new HashMap<>() {{
            put(exporterGroupTimeMetric.name, exporterGroupTimeMetric);
            put(producerOffsetMetric.name, producerOffsetMetric);
            put(consumerOffsetMetric.name, consumerOffsetMetric);
            put(consumerLagMetric.name, consumerLagMetric);
            put(consumerGroupOffsetMetric.name, consumerGroupOffsetMetric);
            put(consumerGroupLagMetric.name, consumerGroupLagMetric);
        }};
    }
}