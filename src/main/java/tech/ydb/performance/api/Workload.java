package tech.ydb.performance.api;

import java.util.List;

/**
 *
 * @author Aleksandr Gorshenin
 */
public interface Workload extends Runnable {
    public List<Metric> metrics();
}
