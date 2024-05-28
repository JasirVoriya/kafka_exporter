package cn.voriya.kafka.metrics;

import cn.voriya.kafka.metrics.collectors.KafkaCollector;
import cn.voriya.kafka.metrics.config.Config;
import cn.voriya.kafka.metrics.config.ConfigCluster;
import io.prometheus.client.exporter.HTTPServer;
import lombok.extern.log4j.Log4j2;

@Log4j2
public class ExporterApplication {
    public static void main(String[] args) {
        Config.parseConfig(Config.getDefaultConfigPath() + "conf.yaml");
        Config config = Config.getInstance();
        int port = Integer.parseInt(config.getPort());
        for (ConfigCluster configCluster : config.getCluster()) {
            log.info("cluster: {}", configCluster);
        }
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
