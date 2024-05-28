package cn.voriya.kafka.metrics.config;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ConfigCluster {
    private String name;
    private List<String> brokers;
}
