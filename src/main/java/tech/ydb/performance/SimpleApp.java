package tech.ydb.performance;

import java.text.DecimalFormat;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tech.ydb.performance.api.Metric;
import tech.ydb.performance.api.Workload;
import tech.ydb.performance.api.YdbRuntime;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class SimpleApp implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(SimpleApp.class);

    private final AppConfig config;
    private final YdbRuntime ydb;

    public SimpleApp(AppConfig config) {
        this.config = config;
        this.ydb = AppFactory.createYdbRuntime(config);
    }

    public List<Metric> run() {
        Workload workload = AppFactory.createWorkload(config, ydb);
        workload.run();

        DecimalFormat df = new DecimalFormat("0.#####");
        workload.metrics().forEach(m -> {
            logger.info("metric {} = {}", m.name(), df.format(m.value()));
        });

        return workload.metrics();
    }

    @Override
    public void close() {
        ydb.close();
    }
}
