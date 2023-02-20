package tech.ydb.performance.metrics;

import java.util.Arrays;
import java.util.List;

import tech.ydb.performance.api.Metric;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class CounterMetric {
    private long count = 0;

    public void inc() {
        count += 1;
    }

    void merge(CounterMetric other) {
        this.count += other.count;
    }

    public List<Metric> toMetrics(long ms, String prefix) {
        return Arrays.asList(
                new Metric(prefix + "TOTAL_COUNT", count),
                new Metric(prefix + "TOTAL_MS", ms),
                new Metric(prefix + "COUNT_PER_SECOND", 1000d * count / ms)
        );
    }
}
