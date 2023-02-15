package tech.ydb.performance.api;

import java.util.List;

/**
 *
 * @author Aleksandr Gorshenin
 */
public interface Workload extends Runnable {
    public interface Metric {
        public String name();
        public double value();
    }

    public List<Metric> metrics();
}
