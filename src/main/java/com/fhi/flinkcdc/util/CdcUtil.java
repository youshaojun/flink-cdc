package com.fhi.flinkcdc.util;

import com.fhi.flinkcdc.service.impl.Kafka2MySQLCdcImpl;
import lombok.SneakyThrows;

import java.util.Map;
import java.util.concurrent.*;

public class CdcUtil {

    public static final int MAX_JOB_COUNT = 128;

    public static final Map<String, Object> JOB_LOCK = new ConcurrentHashMap<>(MAX_JOB_COUNT);

    private static final ExecutorService executorService = new ThreadPoolExecutor(MAX_JOB_COUNT, MAX_JOB_COUNT, 30, TimeUnit.SECONDS, new LinkedBlockingDeque<>(1));

    public static void tryLock(String jobName, Object value) {
        synchronized (Kafka2MySQLCdcImpl.class) {
            if (JOB_LOCK.containsKey(jobName)) {
                throw new RuntimeException(String.format("[%s]任务已存在不可重复提交", jobName));
            }
            if (MAX_JOB_COUNT <= JOB_LOCK.size() + 1) {
                throw new RuntimeException(String.format("同时支持任务最大数量为[%d]", MAX_JOB_COUNT));
            }
            JOB_LOCK.put(jobName, value);
        }
    }

    public static void run(Runnable runnable) {
        executorService.execute(runnable);
    }

    @SneakyThrows
    public static void close(String jobName) {
        Object instance = JOB_LOCK.get(jobName);
        if (instance instanceof Kafka2MySQLCdcImpl.Kafka2MySQLWriter) {
            ((Kafka2MySQLCdcImpl.Kafka2MySQLWriter) instance).close();
        }
        JOB_LOCK.remove(jobName);
    }

}
