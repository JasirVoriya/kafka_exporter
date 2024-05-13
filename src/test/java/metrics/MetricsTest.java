package metrics;

import cn.voriya.kafka.metrics.entity.ConsumerTopicPartitionOffsetMetric;
import cn.voriya.kafka.metrics.entity.TopicPartitionOffsetMetric;
import cn.voriya.kafka.metrics.job.ConsumerTopicPartitionOffset;
import cn.voriya.kafka.metrics.job.TopicPartitionOffset;
import lombok.SneakyThrows;

import java.util.ArrayList;

public class MetricsTest {
    private static final String brokerList = "10.178.75.211:9092,10.178.75.212:9092,10.178.75.213:9092";

    @SneakyThrows
    public static void main(String[] args) {
//        统计执行时间
        long start = System.currentTimeMillis();
        {
            ArrayList<TopicPartitionOffsetMetric> topicPartitionOffsetMetrics = TopicPartitionOffset.get(brokerList);
            System.out.println(topicPartitionOffsetMetrics.size());
            long end = System.currentTimeMillis();
            System.out.println("Time: " + (end - start) + "ms");
        }
        {
            ArrayList<ConsumerTopicPartitionOffsetMetric> consumerTopicPartitionOffsetMetrics = ConsumerTopicPartitionOffset.get(brokerList);
            System.out.println(consumerTopicPartitionOffsetMetrics.size());
            long end = System.currentTimeMillis();
            System.out.println("Time: " + (end - start) + "ms");
        }
        System.out.println("Done");
    }

    void printTopicPartitionOffsetMetrics(ArrayList<TopicPartitionOffsetMetric> topicPartitionOffsetMetrics) {
        String format = "%-30s %-10s %-15s %s";
        String title = String.format(format, "TOPIC", "PARTITION", "OFFSET", "LEADER");  // 生成表头
        System.out.println(title);
        for (TopicPartitionOffsetMetric metric : topicPartitionOffsetMetrics) {
            String line = String.format(format, metric.getTopic(), metric.getPartition(), metric.getOffset(), metric.getLeader());
            System.out.println(line);
        }
    }

    void printConsumerTopicPartitionOffsetMetrics(ArrayList<ConsumerTopicPartitionOffsetMetric> consumerTopicPartitionOffsetMetrics) {
        String format = "%-30s %-30s %-10s %-30s %-15s %-15s %-10s %-50s %-30s %s";
        String title = String.format(format, "CONSUMER-GROUP", "TOPIC", "PARTITION", "COORDINATOR", "CURRENT-OFFSET", "LOG-END-OFFSET", "LAG", "CONSUMER-ID", "HOST", "CLIENT-ID");  // 生成表头
        System.out.println(title);
        for (ConsumerTopicPartitionOffsetMetric metric : consumerTopicPartitionOffsetMetrics) {
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
