package cn.voriya.kafka.metrics.thread;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ThreadPool {
    public static final ThreadPoolExecutor CLUSTER_POOL = new ThreadPoolExecutor(5, 5, 0,
            TimeUnit.MINUTES,
            new ArrayBlockingQueue<>(1000),
            new ThreadFactoryBuilder().setNameFormat("kafka-clusters-%d").setDaemon(true).build());
    public static final ExecutorService VIRTUAL_EXECUTOR = new ThreadPoolExecutor(
            Runtime.getRuntime().availableProcessors() * 20,
            Runtime.getRuntime().availableProcessors() * 20,
            0,
            TimeUnit.MINUTES,
            new ArrayBlockingQueue<>(1000),
            new ThreadFactoryBuilder().setNameFormat("kafka-clusters-%d").setDaemon(true).build()
    );
}
