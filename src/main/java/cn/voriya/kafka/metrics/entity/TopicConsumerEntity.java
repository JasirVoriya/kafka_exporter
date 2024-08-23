package cn.voriya.kafka.metrics.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class TopicConsumerEntity {
    private String from;
    private String consumerGroup;
    private String topic;
    private Integer partition;
    private String leader;
    private String coordinator;
    private Long offset;
    private Long logEndOffset;
    private Long lag;
    private String consumerId;
    private String host;
    private String clientId;
}
