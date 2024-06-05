package cn.voriya.kafka.metrics.thread;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ThreadPool {
    public static final ThreadPoolExecutor CONSUMER_METRICS_POOL = new ThreadPoolExecutor(
            Runtime.getRuntime().availableProcessors() * 2,
            Runtime.getRuntime().availableProcessors() * 2,
            0,
            TimeUnit.MINUTES,
            new ArrayBlockingQueue<>(1000),
            new ThreadFactoryBuilder().setNameFormat("kafka-metrics-%d").setDaemon(true).build());
    public static final ThreadPoolExecutor CLUSTER_POOL = new ThreadPoolExecutor(10, 10, 0,
            TimeUnit.MINUTES,
            new ArrayBlockingQueue<>(1000),
            new ThreadFactoryBuilder().setNameFormat("kafka-clusters-%d").setDaemon(true).build());
}
