package cn.voriya.kafka.metrics;

import cn.voriya.kafka.metrics.collectors.KafkaCollector;
import io.prometheus.client.exporter.HTTPServer;
import lombok.extern.slf4j.Slf4j;

import java.util.TimeZone;

@Slf4j
public class ExporterApplication {
    public static void main(String[] args) {
        TimeZone.setDefault(TimeZone.getTimeZone("Asia/Shanghai"));
        if(args.length <1 ) {
            log.error("Usage: java -jar kafka-exporter.jar <broker-list>");
            System.exit(1);
        }
        log.info("broker list: {}", args[0]);
        Config.BROKER_LIST = args[0];
        try (HTTPServer ignored = new HTTPServer(1234)) {
            new KafkaCollector().register();
            log.info("server started on port 1234");
            log.info("Kafka Exporter started");
            Thread.currentThread().join();
        } catch (Throwable t) {
            log.error("Exception starting", t);
        } finally {
            log.info("Kafka Exporter exiting");
        }
    }
}
