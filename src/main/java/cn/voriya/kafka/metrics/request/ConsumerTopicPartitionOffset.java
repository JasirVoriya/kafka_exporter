package cn.voriya.kafka.metrics.request;

import cn.voriya.kafka.metrics.column.MissColumnValues;
import cn.voriya.kafka.metrics.config.ConfigCluster;
import cn.voriya.kafka.metrics.entity.ConsumerTopicPartitionOffsetMetric;
import cn.voriya.kafka.metrics.thread.ThreadPool;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.common.Node;
import scala.collection.JavaConverters;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static kafka.admin.ConsumerGroupCommand.*;

@Log4j2
public class ConsumerTopicPartitionOffset {
    public static ArrayList<ConsumerTopicPartitionOffsetMetric> get(ConfigCluster configCluster) {
        ArrayList<ConsumerTopicPartitionOffsetMetric> metrics = new ArrayList<>();
        ArrayList<Future<ArrayList<ConsumerTopicPartitionOffsetMetric>>> futures = new ArrayList<>();
        //获取所有消费者组
        List<String> groups = listGroups(configCluster);
        for (String group : groups) {
            //多线程，每个消费者组一个线程，获取消费者组的消费信息
            futures.add(ThreadPool.CONSUMER_METRICS_POOL.submit(() -> getGroupMetric(configCluster, group)));
        }
        //获取所有消费者组的消费信息，合并到一个列表
        for (var future : futures) {
            try {
                metrics.addAll(future.get());
            } catch (InterruptedException | ExecutionException e) {
                log.error("Failed to get consumer group metrics", e);
            }
        }
        //返回所有消费者组的消费信息
        return metrics;
    }

    private static ArrayList<ConsumerTopicPartitionOffsetMetric> getGroupMetric(ConfigCluster configCluster, String group) {
        ArrayList<ConsumerTopicPartitionOffsetMetric> metrics = new ArrayList<>();
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
            log.info("group: {}, topic: {}, partition: {}, offset: {}, logEndOffset: {}, lag: {}, consumerId: {}, host: {}, clientId: {}",
                    group, topic, partition, offset, logEndOffset, lag, consumerId, host, clientId);
            var metric = new ConsumerTopicPartitionOffsetMetric(
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
            //如果没有clientId，说明是某个下线的消费者，数据无意义，直接跳过
            if (metric.getClientId().equals(MissColumnValues.STRING.VALUE)) {
                continue;
            }
            //同一个topic，同一个partition，且如果当前消费者的offset>=之前消费者的offset，说明之前的消费者停止消费，需要移除
            boolean needAdd = true;
            for (var m : metrics) {
                if (m.getTopic().equals(metric.getTopic()) && m.getPartition().equals(metric.getPartition())) {
                    if (m.getOffset() <= metric.getOffset()) {
                        metrics.remove(m);
                    } else {
                        needAdd = false;
                    }
                    break;
                }
            }
            if (needAdd) {
                metrics.add(metric);
            }
        }
        //返回消费者组的消费信息
        return metrics;
    }

    private static List<String> listGroups(ConfigCluster configCluster) {
        String brokerList = String.join(",", configCluster.getBrokers());
        String[] args = {"--bootstrap-server", brokerList};
        List<String> groups = new ArrayList<>();
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
                return new ArrayList<>();
            }
            return JavaConverters.seqAsJavaListConverter(describeGroup._2().get()).asJava();
        }catch (Exception e) {
            log.error("Failed to describe group, cluster: {}, group: {}", configCluster.getName(), group, e);
            return new ArrayList<>();
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
