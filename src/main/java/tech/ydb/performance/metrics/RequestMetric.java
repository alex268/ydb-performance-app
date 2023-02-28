package tech.ydb.performance.metrics;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import tech.ydb.performance.api.Metric;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class RequestMetric {
    private final TimingMetric oks = new TimingMetric();
    private final TimingMetric errors = new TimingMetric();

    public void record(boolean ok, long ns) {
        if (ok) {
            oks.record(ns);
        } else {
            errors.record(ns);
        }
    }

    public void merge(RequestMetric other) {
        oks.merge(other.oks);
        errors.merge(other.errors);
    }

    public List<Metric> toMetrics(String name) {
        return Stream.of(
                oks.toMetrics(name + "_OK_"),
                errors.toMetrics(name + "_ERROR_")
        ).flatMap(List::stream).collect(Collectors.toList());
    }
}
