package tech.ydb.performance.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import tech.ydb.performance.AppConfig;
import tech.ydb.performance.api.AppRecord;
import tech.ydb.performance.api.Metric;
import tech.ydb.performance.api.Workload;
import tech.ydb.performance.api.YdbRuntime;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class LoadWorkload implements Workload {
    private final AppConfig config;
    private final YdbRuntime ydb;
    private final TimingMetric timing = new TimingMetric();

    public LoadWorkload(AppConfig config, YdbRuntime ydb) {
        this.config = config;
        this.ydb = ydb;
    }

    @Override
    public List<Metric> metrics() {
        return timing.toMetrics();
    }

    @Override
    public void run() {
        ExecutorService executor = Executors.newFixedThreadPool(config.threadsCount(), new NamedThreadFactory("load"));
        List<CompletableFuture<TimingMetric>> taskTimings = new ArrayList<>();

        long first = 0;
        int perThread = config.recordCount() / config.threadsCount();
        for (int idx = 1; idx <= config.threadsCount(); idx += 1) {
            long last = config.recordCount() - perThread * (config.threadsCount() - idx);

            LoadTask task = new LoadTask(first, last);
            taskTimings.add(CompletableFuture.supplyAsync(task::call, executor));
            first = last;
        }

        // collect all timings
        taskTimings.forEach(future -> timing.record(future.join()));
    }

    private class LoadTask implements Callable<TimingMetric> {
        private final long startID;
        private final long lastID;

        public LoadTask(long startID, long lastID) {
            this.startID = startID;
            this.lastID = lastID;
        }

        @Override
        public TimingMetric call() {
            TimingMetric timing = new TimingMetric();

            long idx = startID;
            while (idx < lastID) {
                long size = Math.min(lastID - idx, config.batchSize());
                timing.record(bulkUpsert(idx, size));
                idx += size;
            }

            return timing;
        }

        private long bulkUpsert(long idx, long size) {
            List<AppRecord> batch = LongStream.range(idx, idx + size)
                    .mapToObj(i -> AppRecord.createByIndex(i, config.recordSize()))
                    .collect(Collectors.toList());

            long before = System.nanoTime();
            ydb.bulkUpsert(batch).join();
            long after = System.nanoTime();

            return after - before;
        }
    }
}
