package cn.voriya.kafka.metrics.entity;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class ConsumerTopicPartitionOffsetMetric {
    public static final String[] HEADERS = new String[] {
        "consumer_group",
        "topic",
        "partition",
        "coordinator",
        "consumer_id",
        "host",
        "client_id"
    };
    public static final String METRIC_NAME_OFFSET = "kafka_consumer_topic_partition_offset";
    public static final String METRIC_NAME_LAG = "kafka_consumer_topic_partition_lag";
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
            consumerId,
            host,
            clientId
        };
    }
}
