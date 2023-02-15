package tech.ydb.performance.impl;

import java.util.Collections;
import java.util.List;

import tech.ydb.performance.AppConfig;
import tech.ydb.performance.api.Workload;
import tech.ydb.performance.api.YdbRuntime;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class LoadWorkload implements Workload {
    private final AppConfig config;
    private final YdbRuntime ydb;

    public LoadWorkload(AppConfig config, YdbRuntime ydb) {
        this.config = config;
        this.ydb = ydb;
    }

    @Override
    public List<Metric> metrics() {
        return Collections.emptyList();
    }

    @Override
    public void run() {

    }
}
