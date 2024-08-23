package cn.voriya.kafka.metrics.zk;

import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.ZooKeeper;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class ZkUtils {
    private ZooKeeper zooKeeper;
    private final String consumerDir = "/consumers";

    public ZkUtils(String zkUrl, Integer sessionTimeout) {
        try {
            zooKeeper = new ZooKeeper(zkUrl, sessionTimeout, _ -> {
            });
        } catch (Exception e) {
            log.error("ZkUtils error", e);
        }
    }

    public List<String> getTopicsByConsumerGroup(String group) {
        try {
            return zooKeeper.getChildren(STR."\{consumerDir}/\{group}/offsets", false);
        } catch (Exception e) {
            log.error("getTopicsByConsumerGroup error", e);
        }
        return new ArrayList<>();
    }

    public Map<String, Long> getConsumerOffset(String group, String topic) {
        String topicPath = STR."\{consumerDir}/\{group}/offsets/\{topic}";
        try {
            List<String> partitions = zooKeeper.getChildren(topicPath, false);
            Map<String, Long> datas = new HashMap<>();
            for (String partition : partitions) {
                byte[] data = zooKeeper.getData(STR."\{topicPath}/\{partition}", false, null);
                datas.put(partition, Long.parseLong(new String(data, StandardCharsets.UTF_8)));
            }
            return datas;
        } catch (Exception e) {
            log.error("getConsumerOffset error", e);
        }
        return new HashMap<>();
    }

    public List<String> getConsumers() {
        try {
            return zooKeeper.getChildren(consumerDir, false);
        } catch (Exception e) {
            log.error("getConsumers error", e);
        }
        return new ArrayList<>();
    }
}