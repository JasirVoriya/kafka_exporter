package metrics;

import cn.voriya.kafka.metrics.config.ConfigCluster;
import cn.voriya.kafka.metrics.entity.TopicConsumerOffsetMetric;
import cn.voriya.kafka.metrics.entity.TopicProducerOffsetMetric;
import cn.voriya.kafka.metrics.request.TopicConsumerOffset;
import cn.voriya.kafka.metrics.request.TopicPartitionOffset;
import lombok.SneakyThrows;

import java.util.ArrayList;
import java.util.Arrays;

public class MetricsTest {
    private static final String brokerList = "10.178.75.211:9092,10.178.75.212:9092,10.178.75.213:9092";
    private static final ConfigCluster configCluster = new ConfigCluster() {{
        setBrokers(Arrays.asList(brokerList.split(",")));
        setName("dev1");//
    }};

    @SneakyThrows
    public static void main(String[] args) {
//        统计执行时间
        long start = System.currentTimeMillis();
        {
            ArrayList<TopicProducerOffsetMetric> topicProducerOffsetMetrics = TopicPartitionOffset.get(configCluster);
            System.out.println(topicProducerOffsetMetrics.size());
            long end = System.currentTimeMillis();
            System.out.println("Time: " + (end - start) + "ms");
        }
        {
            ArrayList<TopicConsumerOffsetMetric> topicConsumerOffsetMetrics = TopicConsumerOffset.get(configCluster);
            System.out.println(topicConsumerOffsetMetrics.size());
            long end = System.currentTimeMillis();
            System.out.println("Time: " + (end - start) + "ms");
        }
        System.out.println("Done");
    }

    void printTopicPartitionOffsetMetrics(ArrayList<TopicProducerOffsetMetric> topicProducerOffsetMetrics) {
        String format = "%-30s %-10s %-15s %s";
        String title = String.format(format, "TOPIC", "PARTITION", "OFFSET", "LEADER");  // 生成表头
        System.out.println(title);
        for (TopicProducerOffsetMetric metric : topicProducerOffsetMetrics) {
            String line = String.format(format, metric.getTopic(), metric.getPartition(), metric.getOffset(), metric.getLeader());
            System.out.println(line);
        }
    }

    void printConsumerTopicPartitionOffsetMetrics(ArrayList<TopicConsumerOffsetMetric> topicConsumerOffsetMetrics) {
        String format = "%-30s %-30s %-10s %-30s %-15s %-15s %-10s %-50s %-30s %s";
        String title = String.format(format, "CONSUMER-GROUP", "TOPIC", "PARTITION", "COORDINATOR", "CURRENT-OFFSET", "LOG-END-OFFSET", "LAG", "CONSUMER-ID", "HOST", "CLIENT-ID");  // 生成表头
        System.out.println(title);
        for (TopicConsumerOffsetMetric metric : topicConsumerOffsetMetrics) {
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
