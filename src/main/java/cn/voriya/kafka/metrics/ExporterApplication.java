package cn.voriya.kafka.metrics;

import cn.voriya.kafka.metrics.collectors.KafkaCollector;
import cn.voriya.kafka.metrics.config.Config;
import cn.voriya.kafka.metrics.config.ConfigCluster;
import io.prometheus.client.exporter.HTTPServer;
import lombok.extern.slf4j.Slf4j;

import java.util.TimeZone;

@Slf4j
public class ExporterApplication {
    public static void main(String[] args) {
        TimeZone.setDefault(TimeZone.getTimeZone("Asia/Shanghai"));
        Config.parseConfig(Config.getDefaultConfigPath() + "config.yaml");
        Config config = Config.getInstance();
        int port = Integer.parseInt(config.getPort());
        for (ConfigCluster configCluster : config.getCluster()) {
            log.info("cluster: {}", configCluster);
        }
        log.info("port: {}", port);
        try (HTTPServer ignored = new HTTPServer(port)) {
            new KafkaCollector().register();
            log.info("server started on port {}", port);
            log.info("Kafka Exporter started");
            Thread.currentThread().join();
        } catch (Throwable t) {
            log.error("Exception starting", t);
        } finally {
            log.info("Kafka Exporter exiting");
        }
    }
}
