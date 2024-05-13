package cn.voriya.kafka.metrics.entity;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class TopicPartitionOffsetMetric {
    public static final String[] HEADERS = new String[] {
        "topic",
        "partition",
        "leader"
    };
    public static final String METRIC_NAME = "kafka_topic_partition_offset";
    private String topic;
    private Integer partition;
    private Long offset;
    private String leader;

    public String[] toArray() {
        return new String[] {
            topic,
            String.valueOf(partition),
            leader
        };
    }
}
