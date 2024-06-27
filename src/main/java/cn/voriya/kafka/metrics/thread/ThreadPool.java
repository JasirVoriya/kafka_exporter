package cn.voriya.kafka.metrics.thread;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.util.concurrent.*;

public class ThreadPool {
    public static final ThreadPoolExecutor CLUSTER_POOL = new ThreadPoolExecutor(10, 10, 0,
            TimeUnit.MINUTES,
            new ArrayBlockingQueue<>(1000),
            new ThreadFactoryBuilder().setNameFormat("kafka-clusters-%d").setDaemon(true).build());
    public static final ExecutorService VIRTUAL_EXECUTOR = Executors.newFixedThreadPool(
            Runtime.getRuntime().availableProcessors() * 10000,
            Thread.ofVirtual().factory()
    );
}
