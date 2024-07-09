package cn.voriya.kafka.metrics.request;

import cn.voriya.kafka.metrics.column.MissColumnValues;
import cn.voriya.kafka.metrics.config.Config;
import cn.voriya.kafka.metrics.config.ConfigCluster;
import cn.voriya.kafka.metrics.entity.TopicConsumerEntity;
import cn.voriya.kafka.metrics.entity.TopicGroupEntity;
import cn.voriya.kafka.metrics.thread.SchedulerPool;
import cn.voriya.kafka.metrics.thread.ThreadPool;
import kafka.admin.AdminClient;
import kafka.coordinator.group.GroupOverview;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.Node;
import org.apache.zookeeper.ZooKeeper;
import scala.collection.JavaConverters;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static kafka.admin.ConsumerGroupCommand.*;

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
        Set<String> groupCache = clusterGroupsCache.getOrDefault(configCluster.getName(), ConcurrentHashMap.newKeySet());
        log.info("Get consumer groups, cluster: {}, groups: {}", configCluster.getName(), clusterGroupsCache);
        for (String group : groupCache) {
            //多线程，每个消费者组一个线程，获取消费者组的消费信息
            futures.add(ThreadPool.CONSUMER_POOL.submit(() -> getGroupMetric(configCluster, group)));
        }
        //获取所有消费者组的消费信息，合并到一个列表
        for (var future : futures) {
            try {
                metrics.add(future.get());
            } catch (InterruptedException | ExecutionException e) {
                log.error("Failed to get consumer group metrics", e);
            }
        }
        //返回所有消费者组的消费信息
        return metrics;
    }

    private static TopicGroupEntity getGroupMetric(ConfigCluster configCluster, String group) {
        TopicGroupEntity topicGroup = new TopicGroupEntity();
        List<TopicConsumerEntity> consumers = new LinkedList<>();
        topicGroup.setConsumers(consumers);
        topicGroup.setCluster(configCluster.getName());
        topicGroup.setGroup(group);
        StopWatch totalStopWatch = StopWatch.createStarted();
        //请求消费者组信息
        var partitionAssignmentStateList = getGroupDescribe(configCluster, group);
        for (var partitionAssignmentState : partitionAssignmentStateList) {
            String topic = partitionAssignmentState.topic().getOrElse(MissColumnValues.STRING);
            Integer partition = partitionAssignmentState.partition().getOrElse(MissColumnValues.INTEGER);
            Node coordinator = partitionAssignmentState.coordinator().getOrElse(MissColumnValues.NODE);
            Long offset = partitionAssignmentState.offset().getOrElse(MissColumnValues.LONG);
            Long logEndOffset = partitionAssignmentState.logEndOffset().getOrElse(MissColumnValues.LONG);
            Long lag = partitionAssignmentState.lag().getOrElse(MissColumnValues.LONG);
            String consumerId = partitionAssignmentState.consumerId().getOrElse(MissColumnValues.STRING);
            String host = partitionAssignmentState.host().getOrElse(MissColumnValues.STRING);
            String clientId = partitionAssignmentState.clientId().getOrElse(MissColumnValues.STRING);
            log.info("cluster: {}, group: {}, topic: {}, partition: {}, offset: {}, logEndOffset: {}, lag: {}, consumerId: {}, host: {}, clientId: {}",
                    configCluster.getName(), group, topic, partition, offset, logEndOffset, lag, consumerId, host, clientId);
            var metric = new TopicConsumerEntity(
                    group,
                    topic,
                    partition,
                    "No Leader",
                    String.format("%s:%d", coordinator.host(), coordinator.port()),
                    offset,
                    logEndOffset,
                    lag,
                    consumerId,
                    host,
                    clientId);
            consumers.add(metric);
        }
        topicGroup.setTime(totalStopWatch.getTime());
        log.info("Finish to collect consumer group metrics, cluster: {}, group: {}, time: {}ms", configCluster.getName(), group, totalStopWatch.getTime());
        //返回消费者组的消费信息
        return topicGroup;
    }

    private static void refreshGroups() {
        StopWatch stopWatch = StopWatch.createStarted();
        log.info("Start to refresh group list");
        List<ConfigCluster> clusters = Config.getInstance().getCluster();
        for (ConfigCluster cluster : clusters) {
            //获取所有消费者组
            clusterGroupsCache.put(cluster.getName(), listGroups(cluster));
        }
        log.info("Finish to refresh group list, time:{}ms", stopWatch.getTime());
    }

    private static Set<String> listGroups(ConfigCluster configCluster) {
        Set<String> groups = new HashSet<>();
        groups.addAll(listGroupsFromKafkaAdmin(configCluster));
        groups.addAll(listGroupsFromZookeeper(configCluster));
        return groups;
    }

    private static Set<String> listGroupsFromZookeeper(ConfigCluster configCluster) {
        StopWatch stopWatch = StopWatch.createStarted();
        log.info("List groups from zookeeper, cluster: {}", configCluster.getName());
        String zookeeperList = String.join(",", configCluster.getZookeepers());
        Set<String> groups = new HashSet<>();
        ZooKeeper zooKeeper = null;
        try {
            zooKeeper = new ZooKeeper(zookeeperList, 30000, watchedEvent -> {
            });
            groups.addAll(zooKeeper.getChildren("/consumers", false));
        } catch (Exception e) {
            log.error("Failed to list groups, cluster: {}", configCluster.getName(), e);
        } finally {
            try {
                if (zooKeeper != null) {
                    zooKeeper.close();
                }
            } catch (Exception e) {
                log.error("Failed to close zookeeper", e);
            }
        }
        log.info("Finish to list groups from zookeeper, cluster: {}, time: {}ms", configCluster.getName(), stopWatch.getTime());
        return groups;
    }

    private static Set<String> listGroupsFromKafkaAdmin(ConfigCluster configCluster) {
        StopWatch stopWatch = StopWatch.createStarted();
        log.info("List groups from kafka admin, cluster: {}", configCluster.getName());
        String brokerList = String.join(",", configCluster.getBrokers());
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        Set<String> groups = new HashSet<>();
        AdminClient adminClient = null;
        try {
            adminClient = AdminClient.create(properties);
            List<GroupOverview> overviews = JavaConverters.seqAsJavaListConverter(adminClient.listAllGroupsFlattened()).asJava();
            groups.addAll(overviews.stream().map(GroupOverview::groupId).collect(Collectors.toSet()));
        } catch (Exception e) {
            log.error("Failed to list groups, cluster: {}", configCluster.getName(), e);
        } finally {
            if (adminClient != null) {
                adminClient.close();
            }
        }
        log.info("Finish to list groups from kafka admin, cluster: {}, time: {}ms", configCluster.getName(), stopWatch.getTime());
        return groups;
    }

    private static List<PartitionAssignmentState> getGroupDescribe(ConfigCluster configCluster, String group) {
        clusterFailCount.putIfAbsent(configCluster.getName(), new ConcurrentHashMap<>());
        Map<String, Long> failCount = clusterFailCount.get(configCluster.getName());
        String brokerList = String.join(",", configCluster.getBrokers());
        String[] args = {"--bootstrap-server", brokerList, "--group", group, "--describe"};
        ConsumerGroupService consumerGroupService = null;
        try {
            consumerGroupService = getConsumerGroupService(args);
            var describeGroup = consumerGroupService.collectGroupOffsets();
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
        }catch (Exception e) {
            failCount.put(group, failCount.getOrDefault(group, 0L) + 1);
            log.error("Failed to describe group, cluster: {}, group: {}, fail count: {}", configCluster.getName(), group, failCount.get(group), e);
            return new LinkedList<>();
        } finally {
            if (consumerGroupService != null) {
                consumerGroupService.close();
            }
        }
    }

    private static ConsumerGroupService getConsumerGroupService(String[] args) {
        ConsumerGroupCommandOptions commandOptions = new ConsumerGroupCommandOptions(args);
        return new ConsumerGroupService(commandOptions);
    }
}
