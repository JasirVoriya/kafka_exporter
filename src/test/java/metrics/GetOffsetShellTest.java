package metrics;

import cn.voriya.kafka.metrics.config.Config;
import cn.voriya.kafka.metrics.config.ConfigCluster;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class GetOffsetShellTest {
    public static void main(String[] args) {
        Config.parseConfig();
        ConfigCluster configCluster = Config.getInstance().getCluster().getFirst();
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, String.join(",", configCluster.getBrokers()));
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "GetOffsetShell");
        Map<TopicPartition, Long> endOffsets;
        try (KafkaConsumer<byte[], byte[]> kafkaConsumer = new KafkaConsumer<>(properties, new ByteArrayDeserializer(), new ByteArrayDeserializer())) {
            var topics = kafkaConsumer.listTopics().values();
            List<TopicPartition> topicPartitions = new ArrayList<>();
            topics.forEach((partitionInfoList) -> {
                for (PartitionInfo partitionInfo : partitionInfoList) {
                    topicPartitions.add(new TopicPartition(partitionInfo.topic(), partitionInfo.partition()));
                }
            });
            endOffsets = kafkaConsumer.endOffsets(topicPartitions);
        }
        endOffsets.forEach(((topicPartition, offset) -> {
            if (topicPartition.topic().equals("skynet-large")) {
                System.out.printf("%s:%s,%s%n", topicPartition.topic(), topicPartition.partition(), offset);
            }
        }));
    }

}
