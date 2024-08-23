package metrics;

import cn.voriya.kafka.metrics.config.Config;
import cn.voriya.kafka.metrics.config.ConfigCluster;
import cn.voriya.kafka.metrics.entity.TopicGroupEntity;
import cn.voriya.kafka.metrics.entity.TopicProducerEntity;
import cn.voriya.kafka.metrics.request.TopicConsumerOffset;
import cn.voriya.kafka.metrics.request.TopicProducerOffset;
import lombok.SneakyThrows;

import java.util.List;

public class MetricsTest {

    @SneakyThrows
    public static void main(String[] args) {
        Config.parseConfig();
        ConfigCluster configCluster = Config.getInstance().getCluster().getFirst();
//        统计执行时间
        long start = System.currentTimeMillis();
        {
            List<TopicProducerEntity> topicProducerRespons = TopicProducerOffset.get(configCluster);
            System.out.println(topicProducerRespons.size());
            long end = System.currentTimeMillis();
            System.out.println("Time: " + (end - start) + "ms");
        }
        {
            List<TopicGroupEntity> topicGroups = TopicConsumerOffset.get(configCluster);
            System.out.println(topicGroups.size());
            long end = System.currentTimeMillis();
            System.out.println("Time: " + (end - start) + "ms");
        }
        System.out.println("Done");
    }
}
