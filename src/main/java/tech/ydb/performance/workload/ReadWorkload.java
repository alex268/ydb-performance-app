package tech.ydb.performance.workload;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tech.ydb.performance.AppConfig;
import tech.ydb.performance.api.AppRecord;
import tech.ydb.performance.api.Metric;
import tech.ydb.performance.api.Workload;
import tech.ydb.performance.api.YdbRuntime;
import tech.ydb.performance.metrics.MetricTimer;
import tech.ydb.performance.metrics.ReadMetric;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class ReadWorkload implements Workload {
    private static final Logger logger = LoggerFactory.getLogger(ReadWorkload.class);

    private final AppConfig config;
    private final YdbRuntime ydb;
    private final ReadMetric metric = new ReadMetric();
    private final MetricTimer timer = new MetricTimer();

    public ReadWorkload(AppConfig config, YdbRuntime ydb) {
        this.config = config;
        this.ydb = ydb;
    }

    @Override
    public List<Metric> metrics() {
        return metric.toMetrics(timer);
    }

    @Override
    public void run() {
        if (!config.warmupIsDisabled()) {
            logger.info("warnup {} sessions", config.threadsCount());
            List<CompletableFuture<YdbRuntime.YdbSession>> sessions = new ArrayList<>();
            for (int idx = 0; idx < config.threadsCount(); idx += 1) {
                sessions.add(ydb.createSession());
            }
            sessions.forEach(future -> future.join().close());
        }

        logger.info("run read workload with {} threads", config.threadsCount());
        ExecutorService executor = Executors.newFixedThreadPool(config.threadsCount(), new NamedThreadFactory("read"));
        List<CompletableFuture<ReadMetric>> taskTimings = new ArrayList<>();

        long finishTime = System.currentTimeMillis() + 1000 * config.testDurationSeconds();
        timer.start();
        for (int idx = 0; idx < config.threadsCount(); idx += 1) {
            ReadTask task = new ReadTask(finishTime);
            taskTimings.add(CompletableFuture.supplyAsync(task::call, executor));
        }

        logger.info("wait {}s to finish all threads...", config.testDurationSeconds());

        // collect all timings
        taskTimings.forEach(future -> metric.merge(future.join()));

        timer.finish();

        try {
            logger.info("shutdown workload");
            executor.shutdown();
            executor.awaitTermination(1, TimeUnit.MINUTES);
        } catch (InterruptedException ex) {
            logger.error("interrupted", ex);
            Thread.currentThread().interrupt();
        }
    }

    private class ReadTask implements Callable<ReadMetric> {
        private final ThreadLocalRandom rnd = ThreadLocalRandom.current();
        private final long finishTimestamp;

        public ReadTask(long finishTimestamp) {
            this.finishTimestamp = finishTimestamp;
        }

        private AppRecord randomRecord() {
            return AppRecord.createByIndex(rnd.nextLong(config.recordCount()), config.recordSize());
        }

        @Override
        public ReadMetric call() {
            ReadMetric timing = new ReadMetric();

            while (System.currentTimeMillis() < finishTimestamp) {
                AppRecord record = randomRecord();

                long p0 = System.nanoTime();
                try (YdbRuntime.YdbSession session = ydb.createSession().join()) {
                    long p1 = System.nanoTime();
                    timing.recordGetSession(true, p1 - p0);

                    try {
                        AppRecord readed = session.read(record.uuid()).join();
                        long p2 = System.nanoTime();
                        timing.recordReadData(true, p2 - p1);

                        if (!record.equals(readed)) {
                            logger.error("readed wrong record");
                        }

                        timing.requestInc();
                    } catch (RuntimeException ex) {
                        long p2 = System.nanoTime();
                        timing.recordReadData(false, p2 - p1);
                    }
                } catch (RuntimeException ex) {
                    long p1 = System.nanoTime();
                    timing.recordGetSession(false, p1 - p0);
                    logger.warn("can't read record {}", ex.getMessage());
                }
            }

            return timing;
        }
    }

}
