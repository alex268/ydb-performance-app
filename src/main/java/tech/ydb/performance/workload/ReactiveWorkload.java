package tech.ydb.performance.workload;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
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
public class ReactiveWorkload implements Workload {
    private static final Logger logger = LoggerFactory.getLogger(ReactiveWorkload.class);

    private final AppConfig config;
    private final YdbRuntime ydb;
    private final ReadMetric metric = new ReadMetric();

    public ReactiveWorkload(AppConfig config, YdbRuntime ydb) {
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

        int threadCounts = Runtime.getRuntime().availableProcessors();

        logger.info("run reactive read workload with {} threads", threadCounts);
        ExecutorService executor = Executors.newFixedThreadPool(threadCounts, new NamedThreadFactory("reactive"));
        final ConcurrentLinkedQueue<ReadMetric> metrics = new ConcurrentLinkedQueue<>();
        final ThreadLocal<ReadMetric> localMetric = ThreadLocal.withInitial(() -> {
            ReadMetric rm = new ReadMetric();
            metrics.add(rm);
            return rm;
        });

        long finishTime = System.currentTimeMillis() + 1000 * config.testDurationSeconds();
        metric.start();

        List<ReactiveTask> tasks = new ArrayList<>();
        for (int idx = 0; idx < config.threadsCount(); idx += 1) {
            ReactiveTask task = new ReactiveTask(executor, localMetric, finishTime);
            tasks.add(task);
            executor.execute(task);
        }

        logger.info("wait {}s to finish all tasks...", config.testDurationSeconds());

        // collect all timings
        tasks.forEach(ReactiveTask::waitFinish);
        metrics.forEach(m -> metric.merge(m));

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

    private class ReactiveTask implements Runnable {
        private final ExecutorService executor;
        private final ThreadLocal<ReadMetric> totalMetric;
        private final long finishTimestamp;
        private final CompletableFuture<?> finish = new CompletableFuture<>();

        public ReactiveTask(ExecutorService executor, ThreadLocal<ReadMetric> metric, long finishTimestamp) {
            this.executor = executor;
            this.totalMetric = metric;
            this.finishTimestamp = finishTimestamp;
        }

        private AppRecord randomRecord() {
            ThreadLocalRandom rnd = ThreadLocalRandom.current();
            return AppRecord.createByIndex(rnd.nextLong(config.recordCount()), config.recordSize());
        }

        private void complete(ReadMetric metric) {
            totalMetric.get().merge(metric);
            if (System.currentTimeMillis() < finishTimestamp) {
                executor.execute(this);
            } else {
                finish.complete(null);
            }
        }

        public void waitFinish() {
            finish.join();
        }

        @Override
        public void run() {
            final NanoTimer timer = new NanoTimer();
            final ReadMetric metric = new ReadMetric();
            ydb.createSession().whenCompleteAsync((session, th1) -> {
                metric.recordGetSession(th1 == null, timer.next());
                if (session == null) {
                    complete(metric);
                    return;
                }

                AppRecord record = randomRecord();
                timer.next();
                session.read(record.uuid()).whenCompleteAsync((readed, th2) -> {
                    metric.recordReadData(th2 == null, timer.next());
                    if (!record.equals(readed)) {
                        logger.error("readed wrong record");
                    }

                    metric.requestInc();
                    complete(metric);

                    session.close();
                }, executor);
            }, executor);
        }
    }

}

