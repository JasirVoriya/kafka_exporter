package cn.voriya.kafka.metrics.collectors;

import io.prometheus.client.Collector;
import lombok.extern.log4j.Log4j2;

import java.util.ArrayList;
import java.util.List;

@Log4j2
public class TestCollector extends Collector {
    private static final String kafkaExporterTest = "kafka_exporter_test";

    private long lastMillis = 0;
    private long cnt = 1;
    private long point = 1;
    private static final long INTERVAL = 30 * 60 * 1000;

    @Override
    public List<MetricFamilySamples> collect() {
        ArrayList<MetricFamilySamples.Sample> samples = new ArrayList<>();
        samples.add(new MetricFamilySamples.Sample(kafkaExporterTest,
                List.of("tag"),
                List.of("cnt"),
                cnt));
        cnt++;
        if (System.currentTimeMillis() - lastMillis > INTERVAL) {
            lastMillis = System.currentTimeMillis();
            samples.add(new MetricFamilySamples.Sample(kafkaExporterTest,
                    List.of("tag"),
                    List.of("point"),
                    point));
            point += 103;
        }
        return List.of(new MetricFamilySamples(kafkaExporterTest, Type.GAUGE, "kafka_exporter_test", samples));
    }
}