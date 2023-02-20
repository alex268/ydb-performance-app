package tech.ydb.performance;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tech.ydb.performance.api.Workload;
import tech.ydb.performance.api.YdbRuntime;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class SimpleApp implements Runnable, AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(SimpleApp.class);

    public static void main(String... args) throws IOException {
        logger.info("start app");
        AppConfig config = AppConfig.parseArgs(args);
        try (SimpleApp app = new SimpleApp(config)) {
            app.run();
        } catch (Exception e) {
            logger.error("app problem", e);
        }
        logger.info("app finised");
    }

    private final AppConfig config;
    private final YdbRuntime ydb;

    public SimpleApp(AppConfig config) {
        this.config = config;
        this.ydb = AppFactory.createYdbRuntime(config);
    }

    @Override
    public void run() {
        Workload workload = AppFactory.createWorkload(config, ydb);
        workload.run();
        workload.metrics().forEach(m -> {
            logger.info("workload metric {} = {}", m.name(), m.value());
        });
    }

    @Override
    public void close() {
        ydb.close();
    }
}
