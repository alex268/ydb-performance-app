package tech.ydb.performance.impl;

import java.util.Arrays;
import java.util.List;

import tech.ydb.performance.api.Metric;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class TimingMetric {
    private long summaryTime = 0;
    private long minTime = Long.MAX_VALUE;
    private long maxTime = 0;
    private long count = 0;

    public void record(long ns) {
        summaryTime += ns;
        minTime = Math.min(minTime, ns);
        maxTime = Math.max(maxTime, ns);
        count += 1;
    }

    public void record(TimingMetric other) {
        summaryTime += other.summaryTime;
        minTime = Math.min(minTime, other.minTime);
        maxTime = Math.max(maxTime, other.maxTime);
        count += other.count;
    }

    public List<Metric> toMetrics() {
        return Arrays.asList(
                new Metric("TOTAL_COUNT", count),
                new Metric("TOTAL_MS", 1e-6d * summaryTime),
                new Metric("AVG_MS", 1e-6d * summaryTime / count),
                new Metric("MIN_MS", 1e-6d * minTime),
                new Metric("MAX_MS", 1e-6d * maxTime)
        );
    }
}
