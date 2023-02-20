package tech.ydb.performance.workload;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tech.ydb.performance.AppConfig;
import tech.ydb.performance.api.AppRecord;
import tech.ydb.performance.api.Metric;
import tech.ydb.performance.api.Workload;
import tech.ydb.performance.api.YdbRuntime;
import tech.ydb.performance.impl.TimingMetric;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class ReadWorkload implements Workload {
    private static final Logger logger = LoggerFactory.getLogger(LoadWorkload.class);

    private final AppConfig config;
    private final YdbRuntime ydb;
    private final Timings timing = new Timings();

    public ReadWorkload(AppConfig config, YdbRuntime ydb) {
        this.config = config;
        this.ydb = ydb;
    }

    @Override
    public List<Metric> metrics() {
        return Stream.concat(
                timing.getSession.toMetrics("GET_SESSION_").stream(),
                timing.readQuery.toMetrics("READ_").stream()
        ).collect(Collectors.toList());
    }

    @Override
    public void run() {
        logger.info("run read workload with {} threads", config.threadsCount());
        ExecutorService executor = Executors.newFixedThreadPool(config.threadsCount(), new NamedThreadFactory("read"));
        List<CompletableFuture<Timings>> taskTimings = new ArrayList<>();

        for (int idx = 0; idx < config.threadsCount(); idx += 1) {
            ReadTask task = new ReadTask(config.operationsCount() / config.threadsCount());
            taskTimings.add(CompletableFuture.supplyAsync(task::call, executor));
        }

        logger.info("wait all tasks...");

        // collect all timings
        taskTimings.forEach(future -> timing.record(future.join()));

        try {
            logger.info("shutdown workload");
            executor.shutdown();
            executor.awaitTermination(1, TimeUnit.MINUTES);
        } catch (InterruptedException ex) {
            logger.error("interrupted", ex);
            Thread.currentThread().interrupt();
        }
    }

    private class Timings {
        public final TimingMetric getSession = new TimingMetric();
        public final TimingMetric readQuery = new TimingMetric();

        public void record(Timings other) {
            getSession.record(other.getSession);
            readQuery.record(other.readQuery);
        }
    }


    private class ReadTask implements Callable<Timings> {
        private final ThreadLocalRandom rnd = ThreadLocalRandom.current();
        private final long operationsCount;

        public ReadTask(long operationsCount) {
            this.operationsCount = operationsCount;
        }

        private AppRecord randomRecord() {
            return AppRecord.createByIndex(rnd.nextLong(config.recordCount()), config.recordSize());
        }

        @Override
        public Timings call() {
            Timings timing = new Timings();

            for (long idx = 0; idx < operationsCount; idx += 1) {
                AppRecord record = randomRecord();

                long p1 = System.nanoTime();
                try (YdbRuntime.YdbSession session = ydb.createSession().join()) {
                    long p2 = System.nanoTime();
                    timing.getSession.record(p2 - p1);

                    session.read(record.uuid()).join();

                    long p3 = System.nanoTime();
                    timing.readQuery.record(p3 - p2);
                } catch (RuntimeException ex) {
                    logger.warn("can't read record {}", ex.getMessage());
                }
            }


            return timing;
        }
    }

}
