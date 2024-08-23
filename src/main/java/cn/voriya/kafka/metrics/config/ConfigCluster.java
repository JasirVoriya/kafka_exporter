package cn.voriya.kafka.metrics.config;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.HashSet;
import java.util.Set;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ConfigCluster {
    private String name;
    private Set<String> brokers;
    private Set<String> zookeepers = new HashSet<>();
    private Set<String> groupBlackList = new HashSet<>();
    private boolean enableZk = true;
}
