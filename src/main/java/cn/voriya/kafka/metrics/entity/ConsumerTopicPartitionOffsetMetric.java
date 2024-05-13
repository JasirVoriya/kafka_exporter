package cn.voriya.kafka.metrics.entity;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class ConsumerTopicPartitionOffsetMetric {
    public static final String[] HEADERS = new String[] {
        "consumer-group",
        "topic",
        "partition",
        "coordinator",
        "offset",
        "logEndOffset",
        "lag",
        "consumerId",
        "host",
        "clientId"
    };
    public static final String METRIC_NAME = "kafka_consumer_topic_partition_offset";
    private String consumerGroup;
    private String topic;
    private Integer partition;
    private String coordinator;
    private Long offset;
    private Long logEndOffset;
    private Long lag;
    private String consumerId;
    private String host;
    private String clientId;

    public String[] toArray() {
        return new String[] {
            consumerGroup,
            topic,
            String.valueOf(partition),
            coordinator,
            String.valueOf(offset),
            String.valueOf(logEndOffset),
            String.valueOf(lag),
            consumerId,
            host,
            clientId
        };
    }
}
