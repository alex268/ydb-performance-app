package tech.ydb.performance.metrics;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class NanoTimer {
    private long currentNanoTime = System.nanoTime();

    public long next() {
        long old = currentNanoTime;
        currentNanoTime = System.nanoTime();
        return currentNanoTime - old;
    }
}
