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
import tech.ydb.performance.metrics.NanoTimer;
import tech.ydb.performance.metrics.ReadMetric;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class MultiReadWorkload implements Workload {
    private static final Logger logger = LoggerFactory.getLogger(MultiReadWorkload.class);
    private static final int READ_COUNT = 10;

    private final AppConfig config;
    private final YdbRuntime ydb;
    private final ReadMetric metric = new ReadMetric();

    public MultiReadWorkload(AppConfig config, YdbRuntime ydb) {
        this.config = config;
        this.ydb = ydb;
    }

    @Override
    public List<Metric> metrics() {
        return metric.toMetrics();
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

        logger.info("run multi read workload with {} threads", config.threadsCount());
        ExecutorService executor = Executors.newFixedThreadPool(config.threadsCount(), new NamedThreadFactory("mread"));
        List<CompletableFuture<ReadMetric>> taskTimings = new ArrayList<>();

        long finishTime = System.currentTimeMillis() + 1000 * config.testDurationSeconds();
        metric.start();
        for (int idx = 0; idx < config.threadsCount(); idx += 1) {
            ReadTask task = new ReadTask(finishTime);
            taskTimings.add(CompletableFuture.supplyAsync(task::call, executor));
        }

        logger.info("wait {}s to finish all threads...", config.testDurationSeconds());

        // collect all timings
        taskTimings.forEach(future -> metric.merge(future.join()));

        metric.finish();

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
            ReadMetric metric = new ReadMetric();
            NanoTimer timer = new NanoTimer();

            while (System.currentTimeMillis() < finishTimestamp) {
                timer.next();
                try (YdbRuntime.YdbSession session = ydb.createSession().join()) {
                    metric.recordGetSession(true, timer.next());

                    for (int readNumber = 0; readNumber < READ_COUNT; readNumber += 1) {
                        AppRecord record = randomRecord();
                        timer.next();
                        try {
                            AppRecord readed = session.read(record.uuid()).join();
                            metric.recordReadData(true, timer.next());

                            if (!record.equals(readed)) {
                                logger.error("readed wrong record");
                            }
                        } catch (RuntimeException ex) {
                            metric.recordReadData(false, timer.next());
                        }
                    }

                    metric.requestInc();
                } catch (RuntimeException ex) {
                    metric.recordGetSession(false, timer.next());
                    logger.warn("can't read record {}", ex.getMessage());
                }
            }

            return metric;
        }
    }

}
