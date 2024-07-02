package cn.voriya.kafka.metrics.request;

import cn.voriya.kafka.metrics.column.MissColumnValues;
import cn.voriya.kafka.metrics.config.ConfigCluster;
import cn.voriya.kafka.metrics.entity.TopicConsumerEntity;
import cn.voriya.kafka.metrics.entity.TopicGroupEntity;
import cn.voriya.kafka.metrics.thread.ThreadPool;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.kafka.common.Node;
import scala.collection.JavaConverters;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static kafka.admin.ConsumerGroupCommand.*;

@Log4j2
public class TopicConsumerOffset {
    public static List<TopicGroupEntity> get(ConfigCluster configCluster) {
        List<TopicGroupEntity> metrics = new LinkedList<>();
        List<Future<TopicGroupEntity>> futures = new LinkedList<>();
        //获取所有消费者组
        List<String> groups = listGroups(configCluster);
        log.info("Get consumer groups, cluster: {}, groups: {}", configCluster.getName(), groups);
        for (String group : groups) {
            //多线程，每个消费者组一个线程，获取消费者组的消费信息
            futures.add(ThreadPool.VIRTUAL_EXECUTOR.submit(() -> getGroupMetric(configCluster, group)));
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

    private static List<String> listGroups(ConfigCluster configCluster) {
        String brokerList = String.join(",", configCluster.getBrokers());
        String[] args = {"--bootstrap-server", brokerList};
        List<String> groups = new LinkedList<>();
        KafkaConsumerGroupService consumerGroupService = null;
        try {
            consumerGroupService = getKafkaConsumerGroupService(args);
            groups = JavaConverters.seqAsJavaListConverter(consumerGroupService.listGroups()).asJava();
        } catch (Exception e) {
            log.error("Failed to list groups, cluster: {}", configCluster.getName(), e);
        } finally {
            if (consumerGroupService != null) {
                consumerGroupService.close();
            }
        }
        return groups;
    }

    private static List<PartitionAssignmentState> getGroupDescribe(ConfigCluster configCluster, String group) {
        String brokerList = String.join(",", configCluster.getBrokers());
        String[] args = {"--bootstrap-server", brokerList, "--group", group, "--describe"};
        KafkaConsumerGroupService consumerGroupService = null;
        try {
            consumerGroupService = getKafkaConsumerGroupService(args);
            var describeGroup = consumerGroupService.describeGroup();
            if (describeGroup._2().isEmpty()) {
                return new LinkedList<>();
            }
            return JavaConverters.seqAsJavaListConverter(describeGroup._2().get()).asJava();
        }catch (Exception e) {
            log.error("Failed to describe group, cluster: {}, group: {}", configCluster.getName(), group, e);
            return new LinkedList<>();
        } finally {
            if (consumerGroupService != null) {
                consumerGroupService.close();
            }
        }
    }

    private static KafkaConsumerGroupService getKafkaConsumerGroupService(String[] args) {
        ConsumerGroupCommandOptions commandOptions = new ConsumerGroupCommandOptions(args);
        return new KafkaConsumerGroupService(commandOptions);
    }
}
