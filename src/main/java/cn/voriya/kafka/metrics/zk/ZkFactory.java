package cn.voriya.kafka.metrics.zk;

import lombok.extern.slf4j.Slf4j;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class ZkFactory {
    private static final Map<String, ZkUtils> ZK_UTILS_MAP = new ConcurrentHashMap<>();
    private static final Integer sessionTimeout = 120000;

    public static ZkUtils getZkUtils(Collection<String> zkUrls) {
        for (String zkUrl : zkUrls) {
            if (!ZK_UTILS_MAP.containsKey(zkUrl)) {
                ZK_UTILS_MAP.put(zkUrl, new ZkUtils(zkUrl, sessionTimeout));
            }
            ZkUtils zkUtils = ZK_UTILS_MAP.get(zkUrl);
            if (zkUtils.getConsumers().isEmpty()) {
                log.warn("reconnect zookeeper:{}", zkUrl);
                ZK_UTILS_MAP.put(zkUrl, new ZkUtils(zkUrl, sessionTimeout));
            }
            return zkUtils;
        }
        throw new RuntimeException("getZkUtils error");
    }
}