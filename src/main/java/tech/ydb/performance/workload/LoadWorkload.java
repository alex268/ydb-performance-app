package tech.ydb.performance.workload;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tech.ydb.performance.AppConfig;
import tech.ydb.performance.api.AppRecord;
import tech.ydb.performance.api.Metric;
import tech.ydb.performance.api.Workload;
import tech.ydb.performance.api.YdbRuntime;
import tech.ydb.performance.metrics.NanoTimer;
import tech.ydb.performance.metrics.RequestMetric;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class LoadWorkload implements Workload {
    private static final Logger logger = LoggerFactory.getLogger(LoadWorkload.class);

    private final AppConfig config;
    private final YdbRuntime ydb;
    private final RequestMetric metric = new RequestMetric();

    public LoadWorkload(AppConfig config, YdbRuntime ydb) {
        this.config = config;
        this.ydb = ydb;
    }

    @Override
    public List<Metric> metrics() {
        return metric.toMetrics("LOAD");
    }

    @Override
    public void run() {
        logger.info("create table...");
        ydb.createTable();

        logger.info("run load workload with {} threads", config.threadsCount());
        ExecutorService executor = Executors.newFixedThreadPool(config.threadsCount(), new NamedThreadFactory("load"));
        List<CompletableFuture<RequestMetric>> taskTimings = new ArrayList<>();

        long first = 0;
        int perThread = config.recordCount() / config.threadsCount();
        long lastBulk = config.recordCount() / config.batchSize();
        AtomicInteger bulkCounter = new AtomicInteger(0);
        for (int idx = 1; idx <= config.threadsCount(); idx += 1) {
            long last = config.recordCount() - perThread * (config.threadsCount() - idx);

            LoadTask task = new LoadTask(first, last, bulkCounter, lastBulk);
            taskTimings.add(CompletableFuture.supplyAsync(task::call, executor));
            first = last;
        }

        logger.info("wait all tasks...");

        // collect all timings
        taskTimings.forEach(future -> metric.merge(future.join()));

        try {
            logger.info("shutdown workload");
            executor.shutdown();
            executor.awaitTermination(1, TimeUnit.MINUTES);
        } catch (InterruptedException ex) {
            logger.error("interrupted", ex);
            Thread.currentThread().interrupt();
        }
    }

    private class LoadTask implements Callable<RequestMetric> {
        private final long startID;
        private final long lastID;
        private final AtomicInteger bulkCounter;
        private final long lastBulk;

        public LoadTask(long startID, long lastID, AtomicInteger bulkCounter, long lastBulk) {
            this.startID = startID;
            this.lastID = lastID;
            this.bulkCounter = bulkCounter;
            this.lastBulk = lastBulk;
        }

        @Override
        public RequestMetric call() {
            RequestMetric metric = new RequestMetric();
            NanoTimer timer = new NanoTimer();

            long idx = startID;
            while (idx < lastID) {
                long size = Math.min(lastID - idx, config.batchSize());
                List<AppRecord> batch = LongStream.range(idx, idx + size)
                        .mapToObj(i -> AppRecord.createByIndex(i, config.recordSize()))
                        .collect(Collectors.toList());

                timer.next();
                Boolean ok = ydb.bulkUpsert(batch).join();
                metric.record(ok, timer.next());

                logger.info("writed {}/{} bulks", bulkCounter.incrementAndGet(), lastBulk);
                idx += size;
            }

            return metric;
        }

    }
}
