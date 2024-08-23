package cn.voriya.kafka.metrics.request;

import cn.voriya.kafka.metrics.config.Config;
import cn.voriya.kafka.metrics.config.ConfigCluster;
import cn.voriya.kafka.metrics.entity.TopicProducerEntity;
import cn.voriya.kafka.metrics.thread.SchedulerPool;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Slf4j
public class TopicProducerOffset {
    private static final String CLIENT_ID = "GetOffsetJavaAPI";
    private static final Map<String, Map<String, List<PartitionInfo>>> clusterTopicsCache = new ConcurrentHashMap<>();

    static {
        refreshTopics();
        SchedulerPool.submit(
                TopicProducerOffset::refreshTopics,
                "refresh topic list",
                5,
                5,
                TimeUnit.MINUTES);
    }

    private static void refreshTopics() {
        StopWatch stopWatch = StopWatch.createStarted();
        log.info("Start to refresh topic list");
        List<ConfigCluster> clusters = Config.getInstance().getCluster();
        for (ConfigCluster cluster : clusters) {
            Properties properties = new Properties();
            properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, String.join(",", cluster.getBrokers()));
            properties.put(ConsumerConfig.CLIENT_ID_CONFIG, CLIENT_ID);
            try (var kafkaConsumer = new KafkaConsumer<>(properties, new ByteArrayDeserializer(), new ByteArrayDeserializer())) {
                try {
                    clusterTopicsCache.put(cluster.getName(), kafkaConsumer.listTopics());
                } catch (Exception e) {
                    log.error("Failed to refresh topic list, cluster: {}", cluster.getName(), e);
                }
            }
        }
        log.info("Refresh topic list finished, cost: {}ms", stopWatch.getTime());
    }

    @SneakyThrows
    public static List<TopicProducerEntity> get(ConfigCluster configCluster) {
        //获取topic元数据
        var topics = clusterTopicsCache.get(configCluster.getName());
        return getTopicProducerEntities(configCluster, topics);
    }

    static List<TopicProducerEntity> getTopicProducerEntities(ConfigCluster configCluster, Collection<String> topics) {
        var clusterTopics = clusterTopicsCache.get(configCluster.getName());
        Map<String, List<PartitionInfo>> topicPartitionMap = clusterTopics.entrySet().stream()
                .filter(entry -> topics.contains(entry.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        return getTopicProducerEntities(configCluster, topicPartitionMap);
    }

    private static List<TopicProducerEntity> getTopicProducerEntities(ConfigCluster configCluster, Map<String, List<PartitionInfo>> topics) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, String.join(",", configCluster.getBrokers()));
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, CLIENT_ID);
        List<TopicProducerEntity> metrics;
        try (var kafkaConsumer = new KafkaConsumer<>(properties, new ByteArrayDeserializer(), new ByteArrayDeserializer())) {
            metrics = new LinkedList<>();
            List<TopicPartition> topicPartitions = new ArrayList<>();
            Map<TopicPartition, Node> topicPartitionNodeMap = new HashMap<>();
            //遍历topic元数据
            for (var entry : topics.entrySet()) {
                String topic = entry.getKey();
                List<PartitionInfo> partitionInfos = entry.getValue();
                for (PartitionInfo partitionInfo : partitionInfos) {
                    if (partitionInfo.leader() == null) {
                        continue;
                    }
                    TopicPartition topicPartition = new TopicPartition(topic, partitionInfo.partition());
                    topicPartitions.add(topicPartition);
                    topicPartitionNodeMap.put(topicPartition, partitionInfo.leader());
                }
            }
            //遍历每个leader的请求信息，开始请求offset

            Map<TopicPartition, Long> endOffsets;
            try {
                endOffsets = kafkaConsumer.endOffsets(topicPartitions);
            } catch (Exception e) {
                log.error("Failed to get producer offset, cluster: {}", configCluster.getName(), e);
                return metrics;
            }
            for (var entry : endOffsets.entrySet()) {
                TopicPartition tp = entry.getKey();
                Long offset = entry.getValue();
                Node leader = topicPartitionNodeMap.get(tp);
                try {
                    metrics.add(new TopicProducerEntity(tp.topic(), tp.partition(), offset, String.format("%s:%d", leader.host(), leader.port())));
                } catch (Exception e) {
                    log.error("Failed to get producer offset, cluster: {}, leader: {}, topic: {}, partition: {}",
                            configCluster.getName(), leader, tp.topic(), tp.partition(), e);
                }
            }
        }
        return metrics;
    }
}
