package cn.voriya.kafka.metrics.request;

import cn.voriya.kafka.metrics.column.MissColumnValues;
import cn.voriya.kafka.metrics.config.Config;
import cn.voriya.kafka.metrics.config.ConfigCluster;
import cn.voriya.kafka.metrics.entity.TopicConsumerEntity;
import cn.voriya.kafka.metrics.entity.TopicGroupEntity;
import cn.voriya.kafka.metrics.entity.TopicProducerEntity;
import cn.voriya.kafka.metrics.thread.SchedulerPool;
import cn.voriya.kafka.metrics.thread.ThreadPool;
import cn.voriya.kafka.metrics.zk.ZkFactory;
import cn.voriya.kafka.metrics.zk.ZkUtils;
import kafka.admin.ConsumerGroupCommand.ConsumerGroupService;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.common.Node;
import scala.collection.JavaConverters;
import scala.collection.immutable.Map$;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static kafka.admin.ConsumerGroupCommand.ConsumerGroupCommandOptions;
import static kafka.admin.ConsumerGroupCommand.PartitionAssignmentState;

@Log4j2
public class TopicConsumerOffset {
    private static final Map<String, Map<String, Long>> clusterFailCount = new ConcurrentHashMap<>();

    private static final Map<String, Set<String>> clusterGroupsCache = new ConcurrentHashMap<>();

    static {
        refreshGroups();
        SchedulerPool.submit(
                TopicConsumerOffset::refreshGroups,
                "refresh group list",
                5,
                5,
                TimeUnit.MINUTES);
    }

    private static void refreshGroups() {
        StopWatch stopWatch = StopWatch.createStarted();
        log.info("Start to refresh group list");
        List<ConfigCluster> clusters = Config.getInstance().getCluster();
        for (ConfigCluster cluster : clusters) {
            //获取所有消费者组
            clusterGroupsCache.put(cluster.getName(), listGroupsFromKafkaAdmin(cluster));
        }
        log.info("Finish to refresh group list, time:{}ms", stopWatch.getTime());
    }

    private static Set<String> listGroupsFromKafkaAdmin(ConfigCluster configCluster) {
        StopWatch stopWatch = StopWatch.createStarted();
        log.info("List groups from kafka admin, cluster: {}", configCluster.getName());
        String brokerList = String.join(",", configCluster.getBrokers());
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        Set<String> groups = new HashSet<>();
        try (AdminClient adminClient = AdminClient.create(properties)) {
            var overviews = adminClient.listConsumerGroups().all().get();
            groups.addAll(overviews.stream().map(ConsumerGroupListing::groupId).collect(Collectors.toSet()));
        } catch (Exception e) {
            log.error("Failed to list groups, cluster: {}", configCluster.getName(), e);
        }
        log.info("Finish to list groups from kafka admin, cluster: {}, time: {}ms", configCluster.getName(), stopWatch.getTime());
        return groups;
    }

    public static Map<String, Long> getClusterFailCount(String cluster, long minCount, long maxCount) {
        Map<String, Long> clusterFailCount = TopicConsumerOffset.clusterFailCount.get(cluster);
        if (clusterFailCount == null) {
            return new HashMap<>();
        }
        Map<String, Long> res = new HashMap<>();
        for (var entry : clusterFailCount.entrySet()) {
            if (entry.getValue() >= minCount && entry.getValue() <= maxCount) {
                res.put(entry.getKey(), entry.getValue());
            }
        }
        return res;
    }

    public static Set<String> getClusterGroupsCache(String cluster) {
        Set<String> res = TopicConsumerOffset.clusterGroupsCache.get(cluster);
        if (res == null) {
            return new HashSet<>();
        }
        return res;
    }
    public static List<TopicGroupEntity> get(ConfigCluster configCluster) {
        List<TopicGroupEntity> metrics = new LinkedList<>();
        List<Future<TopicGroupEntity>> futures = new LinkedList<>();
        //获取所有消费者组
        Set<String> kafkaGroups = clusterGroupsCache.getOrDefault(configCluster.getName(), ConcurrentHashMap.newKeySet());
        log.info("Get consumer groups, cluster: {}, groups: {}", configCluster.getName(), clusterGroupsCache);
        for (String group : kafkaGroups) {
            if (configCluster.getGroupBlackList().contains(group)) {
                log.info("Skip group: {} in black list of cluster: {}", group, configCluster.getName());
                continue;
            }
            //多线程，每个消费者组一个线程，获取消费者组的消费信息
            futures.add(ThreadPool.CONSUMER_POOL.submit(() -> getGroupMetric(configCluster, group)));
        }
        if (configCluster.isEnableZk()) {
            try {
                ZkUtils zkUtils = ZkFactory.getZkUtils(configCluster.getZookeepers());
                List<String> zkGroups = zkUtils.getConsumers();
                for (String group : zkGroups) {
                    if (configCluster.getGroupBlackList().contains(group)) {
                        log.info("Skip group: {} in black list of cluster: {}", group, configCluster.getName());
                        continue;
                    }
                    futures.add(ThreadPool.CONSUMER_POOL.submit(() -> getGroupMetricFromZookeeper(configCluster, group)));
                }
            } catch (Exception e) {
                log.error("Failed to get consumer group metrics from zookeeper, cluster: {}", configCluster.getName(), e);
            }
        }
        //获取所有消费者组的消费信息，合并到一个列表
        for (var future : futures) {
            try {
                metrics.add(future.get());
            } catch (Exception e) {
                log.error("Failed to get consumer group metrics, cluster: {}", configCluster.getName(), e);
            }
        }
        //返回所有消费者组的消费信息
        return metrics;
    }

    private static TopicGroupEntity getGroupMetric(ConfigCluster configCluster, String group) {
        TopicGroupEntity topicGroup = new TopicGroupEntity();
        topicGroup.setCluster(configCluster.getName());
        topicGroup.setGroup(group);
        StopWatch totalStopWatch = StopWatch.createStarted();
        //请求消费者组信息
        var partitionAssignmentStateList = getGroupDescribe(configCluster, group);
        for (var partitionAssignmentState : partitionAssignmentStateList) {
            Node coordinator = partitionAssignmentState.coordinator().getOrElse(MissColumnValues.NODE);
            var metric = new TopicConsumerEntity(
                    "kafka",
                    group,
                    partitionAssignmentState.topic().getOrElse(MissColumnValues.STRING),
                    partitionAssignmentState.partition().getOrElse(MissColumnValues.INTEGER),
                    "No Leader",
                    String.format("%s:%d", coordinator.host(), coordinator.port()),
                    partitionAssignmentState.offset().getOrElse(MissColumnValues.LONG),
                    partitionAssignmentState.logEndOffset().getOrElse(MissColumnValues.LONG),
                    partitionAssignmentState.lag().getOrElse(MissColumnValues.LONG),
                    partitionAssignmentState.consumerId().getOrElse(MissColumnValues.STRING),
                    partitionAssignmentState.host().getOrElse(MissColumnValues.STRING),
                    partitionAssignmentState.clientId().getOrElse(MissColumnValues.STRING));
            topicGroup.getConsumers().add(metric);
        }
        topicGroup.setTime(totalStopWatch.getTime());
        log.info("Finish to collect consumer group metrics from kafka, cluster: {}, group: {}, time: {}ms",
                configCluster.getName(), group, totalStopWatch.getTime());
        //返回消费者组的消费信息
        return topicGroup;
    }

    private static List<PartitionAssignmentState> getGroupDescribe(ConfigCluster configCluster, String group) {
        clusterFailCount.putIfAbsent(configCluster.getName(), new ConcurrentHashMap<>());
        Map<String, Long> failCount = clusterFailCount.get(configCluster.getName());
        String brokerList = String.join(",", configCluster.getBrokers());
        String[] args = {"--bootstrap-server", brokerList, "--group", group, "--describe"};
        ConsumerGroupService consumerGroupService = null;
        try {
            consumerGroupService = getConsumerGroupService(args);
            var describeGroup = consumerGroupService.collectGroupOffsets(group);
            if (describeGroup._2().isEmpty()) {
                failCount.put(group, failCount.getOrDefault(group, 0L) + 1);
                log.error("The consumer group {} does not exist. cluster: {}, fail count: {}", group, configCluster.getName(), failCount.get(group));
                return new LinkedList<>();
            }
            String state = describeGroup._1().get();
            switch (state) {
                case "PreparingRebalance":
                case "AwaitingSync":
                    log.warn("Consumer group {} state is {}, that is rebalancing, cluster: {}", group, state, configCluster.getName());
                case "Empty":
                    log.warn("Consumer group {} state is {}, no active members, cluster: {}", group, state, configCluster.getName());
                case "Stable":
                    failCount.put(group, 0L);
                    return JavaConverters.seqAsJavaListConverter(describeGroup._2().get()).asJava();
                case "Dead":
                default:
                    failCount.put(group, failCount.getOrDefault(group, 0L) + 1);
                    log.error("Consumer group {} state is {}, that does not exist, cluster: {}, fail count: {}", group, state, configCluster.getName(), failCount.get(group));
                    return new LinkedList<>();
            }
        } catch (Exception e) {
            failCount.put(group, failCount.getOrDefault(group, 0L) + 1);
            log.error("Failed to describe group, cluster: {}, group: {}, fail count: {}", configCluster.getName(), group, failCount.get(group), e);
            return new LinkedList<>();
        } finally {
            if (consumerGroupService != null) {
                consumerGroupService.close();
            }
        }
    }


    private static TopicGroupEntity getGroupMetricFromZookeeper(ConfigCluster configCluster, String group) {
        ZkUtils zkUtils = ZkFactory.getZkUtils(configCluster.getZookeepers());
        TopicGroupEntity topicGroup = new TopicGroupEntity();
        topicGroup.setGroup(group);
        topicGroup.setCluster(configCluster.getName());
        StopWatch stopWatch = StopWatch.createStarted();
        List<String> topics = zkUtils.getTopicsByConsumerGroup(group);
        var topicMap = TopicProducerOffset.getTopicProducerEntities(configCluster, topics)
                .stream().collect(Collectors.groupingBy(TopicProducerEntity::getTopic));
        for (String topic : topics) {
            //获取消费者组的消费信息
            Map<String, Long> consumerOffset = zkUtils.getConsumerOffset(group, topic);
            //获取topic的生产者信息
            List<TopicProducerEntity> topicProducerEntities = topicMap.get(topic);
            //如果topic不存在生产者，跳过
            if (topicProducerEntities == null) {
                continue;
            }
            for (TopicProducerEntity producerEntity : topicProducerEntities) {
                TopicConsumerEntity consumerEntity = new TopicConsumerEntity();
                consumerEntity.setFrom("zookeeper");
                consumerEntity.setConsumerGroup(group);
                consumerEntity.setTopic(topic);
                consumerEntity.setPartition(producerEntity.getPartition());
                consumerEntity.setLeader(producerEntity.getLeader());
                consumerEntity.setOffset(consumerOffset.getOrDefault(String.valueOf(producerEntity.getPartition()), 0L));
                consumerEntity.setLogEndOffset(producerEntity.getOffset());
                consumerEntity.setLag(producerEntity.getOffset() - consumerEntity.getOffset());
                topicGroup.getConsumers().add(consumerEntity);
            }
        }
        topicGroup.setTime(stopWatch.getTime());
        log.info("Finish to collect consumer group metrics from zookeeper, cluster: {}, group: {}, time: {}ms",
                configCluster.getName(), group, stopWatch.getTime());
        return topicGroup;
    }

    private static ConsumerGroupService getConsumerGroupService(String[] args) {
        ConsumerGroupCommandOptions commandOptions = new ConsumerGroupCommandOptions(args);
        return new ConsumerGroupService(commandOptions, Map$.MODULE$.empty());
    }
}
