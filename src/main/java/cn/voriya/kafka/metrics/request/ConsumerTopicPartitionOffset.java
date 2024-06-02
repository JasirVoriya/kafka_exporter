package cn.voriya.kafka.metrics.request;

import cn.voriya.kafka.metrics.column.MissColumnValues;
import cn.voriya.kafka.metrics.entity.ConsumerTopicPartitionOffsetMetric;
import cn.voriya.kafka.metrics.thread.ThreadPool;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.common.Node;
import scala.Option;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.Seq;
import scala.collection.immutable.List;
import scala.collection.immutable.List$;

import java.util.ArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static kafka.admin.ConsumerGroupCommand.*;

@Log4j2
public class ConsumerTopicPartitionOffset {
    public static ArrayList<ConsumerTopicPartitionOffsetMetric> get(String brokerList) {
        ArrayList<ConsumerTopicPartitionOffsetMetric> metrics = new ArrayList<>();
        ArrayList<Future<ArrayList<ConsumerTopicPartitionOffsetMetric>>> futures = new ArrayList<>();
        //获取所有消费者组
        List<String> groups = listGroups(brokerList);
        Iterator<String> groupIterator = groups.iterator();
        //遍历消费者组
        while (groupIterator.hasNext()) {
            String group = groupIterator.next();
            //多线程，每个消费者组一个线程，获取消费者组的消费信息
            Future<ArrayList<ConsumerTopicPartitionOffsetMetric>> future = ThreadPool.CONSUMER_METRICS_POOL.submit(() -> getGroupMetric(brokerList, group));
            //添加到future列表
            futures.add(future);
        }
        //获取所有消费者组的消费信息，合并到一个列表
        for (Future<ArrayList<ConsumerTopicPartitionOffsetMetric>> future : futures) {
            ArrayList<ConsumerTopicPartitionOffsetMetric> offsetMetrics;
            try {
                offsetMetrics = future.get();
                metrics.addAll(offsetMetrics);
            } catch (InterruptedException | ExecutionException e) {
                log.error("Failed to get consumer group metrics", e);
            }
        }
        //返回所有消费者组的消费信息
        return metrics;
    }

    private static ArrayList<ConsumerTopicPartitionOffsetMetric> getGroupMetric(String brokerList, String group) {
        ArrayList<ConsumerTopicPartitionOffsetMetric> metrics = new ArrayList<>();
        //请求消费者组信息
        Tuple2<Option<String>, Option<Seq<PartitionAssignmentState>>> groupInfo = getGroupInfo(brokerList, group);
        //获取消费者组每个partition的消费信息
        Seq<PartitionAssignmentState> partitionAssignmentStates = groupInfo._2().get();
        //遍历partition信息，生成metric
        Iterator<PartitionAssignmentState> partitionAssignmentStateIterator = partitionAssignmentStates.iterator();
        while (partitionAssignmentStateIterator.hasNext()) {
            PartitionAssignmentState partitionAssignmentState = partitionAssignmentStateIterator.next();
            String topic = partitionAssignmentState.topic().getOrElse(MissColumnValues.STRING);
            Integer partition = partitionAssignmentState.partition().getOrElse(MissColumnValues.INTEGER);
            Node coordinator = partitionAssignmentState.coordinator().getOrElse(MissColumnValues.NODE);
            Long offset = partitionAssignmentState.offset().getOrElse(MissColumnValues.LONG);
            Long logEndOffset = partitionAssignmentState.logEndOffset().getOrElse(MissColumnValues.LONG);
            Long lag = partitionAssignmentState.lag().getOrElse(MissColumnValues.LONG);
            String consumerId = partitionAssignmentState.consumerId().getOrElse(MissColumnValues.STRING);
            String host = partitionAssignmentState.host().getOrElse(MissColumnValues.STRING);
            String clientId = partitionAssignmentState.clientId().getOrElse(MissColumnValues.STRING);
            ConsumerTopicPartitionOffsetMetric metric = new ConsumerTopicPartitionOffsetMetric(
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
            //过滤无用的消费者
            if (filterConsumer(metrics, metric)) {
                metrics.add(metric);
            }
        }
        //返回消费者组的消费信息
        return metrics;
    }

    private static List<String> listGroups(String brokerList) {
        String[] args = {"--bootstrap-server", brokerList};
        List<String> groups = List$.MODULE$.empty();
        KafkaConsumerGroupService consumerGroupService = null;
        try {
            consumerGroupService = getKafkaConsumerGroupService(args);
            groups = consumerGroupService.listGroups();
        } catch (Exception e) {
            log.error("Failed to list groups", e);
        } finally {
            if (consumerGroupService != null) {
                consumerGroupService.close();
            }
        }
        return groups;
    }

    private static Tuple2<Option<String>, Option<Seq<PartitionAssignmentState>>> getGroupInfo(String brokerList, String group) {
        String[] args = {"--bootstrap-server", brokerList, "--group", group, "--describe"};
        KafkaConsumerGroupService consumerGroupService = null;
        Tuple2<Option<String>, Option<Seq<PartitionAssignmentState>>> describedGroup = new Tuple2<>(Option.empty(), Option.empty());
        try {
            consumerGroupService = getKafkaConsumerGroupService(args);
            describedGroup = consumerGroupService.describeGroup();
        }catch (Exception e) {
            log.error("Failed to describe group", e);
        } finally {
            if (consumerGroupService != null) {
                consumerGroupService.close();
            }
        }
        return describedGroup;
    }

    private static KafkaConsumerGroupService getKafkaConsumerGroupService(String[] args) {
        ConsumerGroupCommandOptions commandOptions = new ConsumerGroupCommandOptions(args);
        return new KafkaConsumerGroupService(commandOptions);
    }

    /**
     * 过滤无用的消费者
     *
     * @param metrics 消费者组的消费信息
     * @param metric  消费者的消费信息
     * @return 被过滤返回false，否则返回true
     */
    private static boolean filterConsumer(ArrayList<ConsumerTopicPartitionOffsetMetric> metrics, ConsumerTopicPartitionOffsetMetric metric) {
        //如果没有clientId，说明是某个下线的消费者，数据无意义，直接跳过
        if (metric.getClientId().equals(MissColumnValues.STRING.VALUE)) {
            return false;
        }
        //如果当前消费者停止消费（同一个partition下有两个消费者，一个消费者停止消费，另一个消费者的offset会变化，导致lag不准确和offset不准确），直接跳过
        for (ConsumerTopicPartitionOffsetMetric m : metrics) {
            //同一个topic，同一个partition，且如果当前消费者的offset小于之前消费者的offset，说明当前消费者停止消费，直接跳过
            if (m.getTopic().equals(metric.getTopic()) && m.getPartition().equals(metric.getPartition()) && metric.getOffset() < m.getOffset()) {
                return false;
            }
        }
        return true;
    }
}
