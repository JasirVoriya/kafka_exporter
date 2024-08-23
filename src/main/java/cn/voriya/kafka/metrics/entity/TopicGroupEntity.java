package cn.voriya.kafka.metrics.entity;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.LinkedList;
import java.util.List;

@Data
@NoArgsConstructor
public class TopicGroupEntity {
    private long time;
    private String cluster;
    private String group;
    private List<TopicConsumerEntity> consumers = new LinkedList<>();
}
