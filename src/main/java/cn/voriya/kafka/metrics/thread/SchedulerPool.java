package cn.voriya.kafka.metrics.thread;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.extern.log4j.Log4j2;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

@Log4j2
public class SchedulerPool {
    private static final ScheduledExecutorService SCHEDULER = Executors.newScheduledThreadPool(10);
    private static final ExecutorService EXECUTOR = Executors.newFixedThreadPool(
            10,
            new ThreadFactoryBuilder().setNameFormat("kafka-scheduler-%d").setDaemon(true).build()
    );

    public static void submit(Runnable command,
                              String taskName,
                              long initialDelay,
                              long period,
                              TimeUnit unit) {
        Lock lock = new ReentrantLock();
        SCHEDULER.scheduleAtFixedRate(() -> EXECUTOR.submit(() -> {
            if (lock.tryLock()) {
                log.info("Start to {}, try lock success", taskName);
                try {
                    command.run();
                } catch (Exception e) {
                    log.error("Failed to {}", taskName, e);
                } finally {
                    lock.unlock();
                    log.info("Finish to {}, unlock success", taskName);
                }
            } else {
                log.error("Failed to {}, try lock failed, maybe last task is running", taskName);
            }
        }), initialDelay, period, unit);
    }
}
