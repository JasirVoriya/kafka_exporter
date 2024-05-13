package cn.voriya.kafka.metrics.job;

import cn.voriya.kafka.metrics.column.MissColumnValues;
import cn.voriya.kafka.metrics.entity.ConsumerTopicPartitionOffsetMetric;
import cn.voriya.kafka.metrics.thread.ThreadPool;
import lombok.SneakyThrows;
import org.apache.kafka.common.Node;
import scala.Option;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.Seq;
import scala.collection.immutable.List;

import java.util.ArrayList;
import java.util.concurrent.Future;

import static kafka.admin.ConsumerGroupCommand.*;

public class ConsumerTopicPartitionOffset {
    @SneakyThrows
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
            ArrayList<ConsumerTopicPartitionOffsetMetric> offsetMetrics = future.get();
            metrics.addAll(offsetMetrics);
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
            Node coordinator = partitionAssignmentState.coordinator().getOrElse(MissColumnValues.getDefault(Node.class));
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
                    String.format("%s:%d", coordinator.host(), coordinator.port()),
                    offset,
                    logEndOffset,
                    lag,
                    consumerId,
                    host,
                    clientId);
            metrics.add(metric);
        }
        //返回消费者组的消费信息
        return metrics;
    }

    private static List<String> listGroups(String brokerList) {
        String[] args = {"--bootstrap-server", brokerList};
        KafkaConsumerGroupService consumerGroupService = getKafkaConsumerGroupService(args);
        return consumerGroupService.listGroups();
    }

    private static Tuple2<Option<String>, Option<Seq<PartitionAssignmentState>>> getGroupInfo(String brokerList, String group) {
        String[] args = {"--bootstrap-server", brokerList, "--group", group, "--describe"};
        KafkaConsumerGroupService consumerGroupService = getKafkaConsumerGroupService(args);
        return consumerGroupService.describeGroup();
    }

    private static KafkaConsumerGroupService getKafkaConsumerGroupService(String[] args) {
        ConsumerGroupCommandOptions commandOptions = new ConsumerGroupCommandOptions(args);
        return new KafkaConsumerGroupService(commandOptions);
    }
}
