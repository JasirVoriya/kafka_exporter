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
            try (KafkaConsumer<byte[], byte[]> kafkaConsumer = new KafkaConsumer<>(properties, new ByteArrayDeserializer(), new ByteArrayDeserializer())) {
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
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, String.join(",", configCluster.getBrokers()));
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, CLIENT_ID);
        List<TopicProducerEntity> metrics;
        //获取topic元数据
        Map<String, List<PartitionInfo>> topics = clusterTopicsCache.get(configCluster.getName());
        try (KafkaConsumer<byte[], byte[]> kafkaConsumer = new KafkaConsumer<>(properties, new ByteArrayDeserializer(), new ByteArrayDeserializer())) {
            metrics = new LinkedList<>();
            List<TopicPartition> topicPartitions = new ArrayList<>();
            Map<TopicPartition, Node> topicPartitionNodeMap = new HashMap<>();
            //遍历topic元数据
            topics.forEach((topic, partitionInfos) -> {
                for (PartitionInfo partitionInfo : partitionInfos) {
                    if (partitionInfo.leader() == null) {
                        continue;
                    }
                    TopicPartition topicPartition = new TopicPartition(topic, partitionInfo.partition());
                    topicPartitions.add(topicPartition);
                    topicPartitionNodeMap.put(topicPartition, partitionInfo.leader());
                }
            });
            //遍历每个leader的请求信息，开始请求offset

            Map<TopicPartition, Long> endOffsets;
            try {
                endOffsets = kafkaConsumer.endOffsets(topicPartitions);
            } catch (Exception e) {
                log.error("Failed to get producer offset, cluster: {}", configCluster.getName(), e);
                return metrics;
            }
            endOffsets.forEach((topicPartition, offset) -> {
                Node leader = topicPartitionNodeMap.get(topicPartition);
                try {
                    metrics.add(new TopicProducerEntity(topicPartition.topic(), topicPartition.partition(), offset, String.format("%s:%d", leader.host(), leader.port())));
                } catch (Exception e) {
                    log.error("Failed to get producer offset, cluster: {}, leader: {}, topic: {}, partition: {}",
                            configCluster.getName(), leader, topicPartition.topic(), topicPartition.partition(), e);
                }
            });
        }
        return metrics;
    }
}
