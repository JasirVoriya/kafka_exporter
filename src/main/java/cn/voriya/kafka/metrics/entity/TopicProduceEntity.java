package cn.voriya.kafka.metrics.entity;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class TopicProduceEntity {
    private String topic;
    private Integer partition;
    private Long offset;
    private String leader;
}
