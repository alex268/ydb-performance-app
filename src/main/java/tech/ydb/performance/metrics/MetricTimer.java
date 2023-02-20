package tech.ydb.performance.metrics;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class MetricTimer {
    private long startTimestamp = 0;
    private long finishTimestamp = 1;

    public void start() {
        this.startTimestamp = System.currentTimeMillis();
        this.finishTimestamp = System.currentTimeMillis() + 1;
    }

    public void finish() {
        this.finishTimestamp = System.currentTimeMillis();
        if (startTimestamp >= finishTimestamp) {
            startTimestamp = finishTimestamp - 1;
        }
    }

    long durationMS() {
        return finishTimestamp - startTimestamp;
    }
}
