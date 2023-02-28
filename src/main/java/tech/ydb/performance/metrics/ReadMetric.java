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

    private long started = System.currentTimeMillis();
    private long finished = started + 1;

    public void start() {
        started = System.currentTimeMillis();
        finished = started + 1;
    }

    public void finish() {
        finished = System.currentTimeMillis();
    }

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

    public List<Metric> toMetrics() {
        return Stream.of(
                counter.toMetrics(finished - started, "REQUESTS_"),
                getSession.toMetrics("GET_SESSION"),
                readData.toMetrics("READ_DATA")
        ).flatMap(List::stream).collect(Collectors.toList());
    }
}
