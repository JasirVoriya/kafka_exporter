package cn.voriya.kafka.metrics.job;

import cn.voriya.kafka.metrics.entity.TopicPartitionOffsetMetric;
import kafka.api.*;
import kafka.client.ClientUtils;
import kafka.cluster.BrokerEndPoint;
import kafka.common.TopicAndPartition;
import kafka.consumer.SimpleConsumer;
import lombok.SneakyThrows;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.Seq;
import scala.collection.immutable.HashMap;
import scala.collection.immutable.ListSet;
import scala.collection.immutable.Map;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;

public class TopicPartitionOffset {
    private static final String CLIENT_ID = "GetOffsetJavaAPI";
    //消费者缓存
    private static final ConcurrentHashMap<BrokerEndPoint, SimpleConsumer> CONSUMER_CACHE = new ConcurrentHashMap<>();
    //请求信息按照leader分组，减少请求次数，一个leader请求一次
    private static final java.util.HashMap<BrokerEndPoint, HashMap<TopicAndPartition, PartitionOffsetRequestInfo>> REQUEST_INFO_MAP = new java.util.HashMap<>();

    private static void putRequestInfo(BrokerEndPoint leader, TopicAndPartition topicAndPartition) {
        if (!REQUEST_INFO_MAP.containsKey(leader)) {
            REQUEST_INFO_MAP.put(leader, new HashMap<>());
        }
        HashMap<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = REQUEST_INFO_MAP.get(leader);
        requestInfo = requestInfo.$plus(new Tuple2<>(topicAndPartition, new PartitionOffsetRequestInfo(OffsetRequest.LatestTime(), 1)));
        REQUEST_INFO_MAP.put(leader, requestInfo);
    }

    @SneakyThrows
    public static ArrayList<TopicPartitionOffsetMetric> get(String brokerList) {
        CONSUMER_CACHE.clear();
        REQUEST_INFO_MAP.clear();
        //解析brokerList
        Seq<BrokerEndPoint> brokerEndPointSeq = ClientUtils.parseBrokerList(brokerList);
        //查询的topic，为空则查询所有topic
        ListSet<String> topics = new ListSet<>();
        //获取topic元数据
        Seq<TopicMetadata> topicMetadataSeq = ClientUtils.fetchTopicMetadata(topics, brokerEndPointSeq, CLIENT_ID, 10000, 10000).topicsMetadata();
        ArrayList<TopicPartitionOffsetMetric> metrics = new ArrayList<>();
        //遍历topic元数据
        Iterator<TopicMetadata> topicMetadataIterator = topicMetadataSeq.iterator();
        while (topicMetadataIterator.hasNext()) {
            TopicMetadata topicMetadata = topicMetadataIterator.next();
            //遍历partition元数据
            Seq<PartitionMetadata> partitionMetadataSeq = topicMetadata.partitionsMetadata();
            Iterator<PartitionMetadata> partitionMetadataIterator = partitionMetadataSeq.iterator();
            while (partitionMetadataIterator.hasNext()) {
                PartitionMetadata partitionMetadata = partitionMetadataIterator.next();
                int partitionId = partitionMetadata.partitionId();
                String topic = topicMetadata.topic();
                //如果没有leader，说明该partition不可用，直接跳过
                if (partitionMetadata.leader().isEmpty()) {
                    metrics.add(new TopicPartitionOffsetMetric(topic, partitionId, 0L, "No Leader"));
                    continue;
                }
                BrokerEndPoint leader = partitionMetadata.leader().get();
                //构建请求信息
                putRequestInfo(leader, new TopicAndPartition(topic, partitionId));
            }
        }
        //遍历每个leader的请求信息，开始请求offset
        for (BrokerEndPoint leader : REQUEST_INFO_MAP.keySet()) {
            //获取leader的SimpleConsumer
            SimpleConsumer consumer = getSimpleConsumer(leader);
            //构建leader的请求信息
            OffsetRequest offsetRequest = new OffsetRequest(REQUEST_INFO_MAP.get(leader), 0, 0);
            //发送请求
            Map<TopicAndPartition, PartitionOffsetsResponse> responseMap = consumer.getOffsetsBefore(offsetRequest).partitionErrorAndOffsets();
            //遍历响应，构建metric
            Iterator<Tuple2<TopicAndPartition, PartitionOffsetsResponse>> responseIterator = responseMap.iterator();
            while (responseIterator.hasNext()) {
                Tuple2<TopicAndPartition, PartitionOffsetsResponse> tuple2 = responseIterator.next();
                TopicAndPartition topicAndPartition = tuple2._1();
                PartitionOffsetsResponse partitionOffsetsResponse = tuple2._2();
                Seq<Object> offsets = partitionOffsetsResponse.offsets();
                Long offset = (Long) offsets.apply(0);
                metrics.add(new TopicPartitionOffsetMetric(topicAndPartition.topic(), topicAndPartition.partition(), offset, String.format("%s:%d", leader.host(), leader.port())));
            }
        }
        return metrics;
    }

    private static SimpleConsumer getSimpleConsumer(BrokerEndPoint leader) {
        if (!CONSUMER_CACHE.containsKey(leader)) {
            CONSUMER_CACHE.put(leader, new SimpleConsumer(leader.host(), leader.port(), 10000, 10000, CLIENT_ID));
        }
        return CONSUMER_CACHE.get(leader);
    }
}
