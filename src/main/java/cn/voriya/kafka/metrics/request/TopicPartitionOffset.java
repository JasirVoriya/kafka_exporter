package cn.voriya.kafka.metrics.request;

import cn.voriya.kafka.metrics.config.ConfigCluster;
import cn.voriya.kafka.metrics.entity.TopicPartitionOffsetMetric;
import kafka.api.*;
import kafka.client.ClientUtils;
import kafka.cluster.BrokerEndPoint;
import kafka.common.TopicAndPartition;
import kafka.consumer.SimpleConsumer;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import scala.collection.JavaConversions;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.util.*;

/**
 * 请求信息按照leader分组，减少请求次数，一个leader请求一次
 */
class RequestInfoMap extends HashMap<BrokerEndPoint, scala.collection.immutable.HashMap<TopicAndPartition, PartitionOffsetRequestInfo>> {
    public void putRequestInfo(BrokerEndPoint leader, TopicAndPartition topicAndPartition) {
        if (!this.containsKey(leader)) {
            this.put(leader, new scala.collection.immutable.HashMap<>());
        }
        scala.collection.immutable.HashMap<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = this.get(leader);
        requestInfo = requestInfo.$plus(new  scala.Tuple2<>(topicAndPartition, new PartitionOffsetRequestInfo(OffsetRequest.LatestTime(), 1)));
        this.put(leader, requestInfo);
    }
}

@Slf4j
public class TopicPartitionOffset {
    private static final String CLIENT_ID = "GetOffsetJavaAPI";

    @SneakyThrows
    public static ArrayList<TopicPartitionOffsetMetric> get(ConfigCluster configCluster) {
        RequestInfoMap requestInfoMap = new RequestInfoMap();
        //获取topic元数据
        List<TopicMetadata> topicMetadataList;
        try {
            topicMetadataList = JavaConverters.seqAsJavaListConverter(ClientUtils.fetchTopicMetadata(
                    JavaConversions.asScalaSet(new HashSet<>()),
                    ClientUtils.parseBrokerList(String.join(",", configCluster.getBrokers())),
                    CLIENT_ID,
                    10000,
                    100000).topicsMetadata()).asJava();
        } catch (Exception e) {
            log.error("Failed to get topic metadata, cluster:{}", configCluster.getName(), e);
            return new ArrayList<>();
        }
        ArrayList<TopicPartitionOffsetMetric> metrics = new ArrayList<>();
        //遍历topic元数据
        for (TopicMetadata topicMetadata : topicMetadataList) {
            //遍历partition元数据
            List<PartitionMetadata> partitionMetadataList = JavaConverters.seqAsJavaListConverter(topicMetadata.partitionsMetadata()).asJava();
            for (PartitionMetadata partitionMetadata : partitionMetadataList) {
                int partitionId = partitionMetadata.partitionId();
                String topic = topicMetadata.topic();
                //如果没有leader，说明该partition不可用，直接跳过
                if (partitionMetadata.leader().isEmpty()) {
                    metrics.add(new TopicPartitionOffsetMetric(topic, partitionId, 0L, "No Leader"));
                    continue;
                }
                BrokerEndPoint leader = partitionMetadata.leader().get();
                //构建请求信息
                requestInfoMap.putRequestInfo(leader, new TopicAndPartition(topic, partitionId));
            }
        }
        //遍历每个leader的请求信息，开始请求offset
        for (BrokerEndPoint leader : requestInfoMap.keySet()) {
            //获取leader的SimpleConsumer
            SimpleConsumer consumer = new SimpleConsumer(leader.host(), leader.port(), 10000, 100000, CLIENT_ID);
            //构建leader的请求信息
            OffsetRequest offsetRequest = new OffsetRequest(requestInfoMap.get(leader), 0, 0);
            //发送请求
            Map<TopicAndPartition, PartitionOffsetsResponse> responseMap = JavaConverters.mapAsJavaMapConverter(consumer.getOffsetsBefore(offsetRequest).partitionErrorAndOffsets()).asJava();
            responseMap.forEach((topicAndPartition, partitionOffsetsResponse) -> {
                Seq<Object> offsets = partitionOffsetsResponse.offsets();
                Long offset = (Long) offsets.apply(0);
                metrics.add(new TopicPartitionOffsetMetric(topicAndPartition.topic(), topicAndPartition.partition(), offset, String.format("%s:%d", leader.host(), leader.port())));
            });
        }
        return metrics;
    }
}
