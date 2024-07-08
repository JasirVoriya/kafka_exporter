package cn.voriya.kafka.metrics.thread;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.extern.log4j.Log4j2;

import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

@Log4j2
public class SchedulerPool {
    private static final ScheduledExecutorService SCHEDULER = Executors.newScheduledThreadPool(10);
    private static final ExecutorService EXECUTOR = Executors.newFixedThreadPool(
            10,
            new ThreadFactoryBuilder().setNameFormat("kafka-scheduler-%d").setDaemon(true).build()
    );
    private static final ConcurrentHashMap<String, Lock> LOCK_MAP = new ConcurrentHashMap<>();

    public static void submit(Runnable command,
                              String taskName,
                              long initialDelay,
                              long period,
                              TimeUnit unit) {
        if (LOCK_MAP.containsKey(taskName)) {
            log.error("Failed to {}, task name is already exists", taskName);
            return;
        }
        LOCK_MAP.put(taskName, new ReentrantLock());
        SCHEDULER.scheduleAtFixedRate(() -> EXECUTOR.submit(() -> {
            Lock lock = LOCK_MAP.get(taskName);
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
