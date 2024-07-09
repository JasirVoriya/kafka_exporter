package metrics;

import cn.voriya.kafka.metrics.config.Config;
import cn.voriya.kafka.metrics.config.ConfigCluster;
import cn.voriya.kafka.metrics.entity.TopicConsumerEntity;
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

    void printTopicPartitionOffsetMetrics(List<TopicProducerEntity> topicProducerRespons) {
        String format = "%-30s %-10s %-15s %s";
        String title = String.format(format, "TOPIC", "PARTITION", "OFFSET", "LEADER");  // 生成表头
        System.out.println(title);
        for (TopicProducerEntity metric : topicProducerRespons) {
            String line = String.format(format, metric.getTopic(), metric.getPartition(), metric.getOffset(), metric.getLeader());
            System.out.println(line);
        }
    }

    void printConsumerTopicPartitionOffsetMetrics(List<TopicConsumerEntity> topicConsumerRespons) {
        String format = "%-30s %-30s %-10s %-30s %-15s %-15s %-10s %-50s %-30s %s";
        String title = String.format(format, "CONSUMER-GROUP", "TOPIC", "PARTITION", "COORDINATOR", "CURRENT-OFFSET", "LOG-END-OFFSET", "LAG", "CONSUMER-ID", "HOST", "CLIENT-ID");  // 生成表头
        System.out.println(title);
        for (TopicConsumerEntity metric : topicConsumerRespons) {
            String line = String.format(format,
                    metric.getConsumerGroup(),
                    metric.getTopic(),
                    metric.getPartition(),
                    metric.getCoordinator(),
                    metric.getOffset(),
                    metric.getLogEndOffset(),
                    metric.getLag(),
                    metric.getConsumerId(),
                    metric.getHost(),
                    metric.getClientId());
            System.out.println(line);
        }
    }
}
