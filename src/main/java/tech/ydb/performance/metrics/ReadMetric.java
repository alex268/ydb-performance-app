package tech.ydb.performance.metrics;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import tech.ydb.performance.api.Metric;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class ReadMetric {
    private final CounterMetric counter = new CounterMetric();

    private final RequestMetric getSession = new RequestMetric();
    private final RequestMetric readData = new RequestMetric();

    public void requestInc() {
        counter.inc();
    }

    public void recordGetSession(boolean ok, long ns) {
        getSession.record(ok, ns);
    }

    public void recordReadData(boolean ok, long ns) {
        readData.record(ok, ns);
    }

    public void merge(ReadMetric other) {
        counter.merge(other.counter);
        getSession.merge(other.getSession);
        readData.merge(other.readData);
    }

    public List<Metric> toMetrics(MetricTimer timer) {
        return Stream.of(
                counter.toMetrics(timer.durationMS(), "REQUESTS_"),
                getSession.toMetrics("GET_SESSION", timer),
                readData.toMetrics("READ_DATA", timer)
        ).flatMap(List::stream).collect(Collectors.toList());
    }
}
